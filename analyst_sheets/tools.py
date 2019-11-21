from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
import os
from nltk.corpus import stopwords
import re
import requests
from retry import retry
import sys
import threading
import time
from web_monitoring.utils import FiniteQueue, Signal


class generate_on_thread(threading.Thread):
    """
    Run a generator on a separate thread, placing the results in to a
    FiniteQueue. The queue is available via `instance.output`.

    Examples
    --------
    Create some random numbers on a separate thread.

    >>> def yield_numbers():
    >>>     for number in range(10)
    >>>         yield number
    >>>
    >>> for number in generate_on_thread(yield_numbers).output:
    >>>     print(number)
    """

    def __init__(self, target, *args, immediate=True, cancel=None, _output=None, **kwargs):
        super().__init__()
        self.should_end = _output is None
        self.output = _output or FiniteQueue()
        self.cancel = cancel
        self.target = target
        self.args = args
        self.kwargs = kwargs
        if immediate:
            self.start()

    def run(self):
        try:
            if self.cancel and self.cancel.is_set():
                return

            iterable = self.target(*self.args, **self.kwargs)
            for item in iterable:
                if self.cancel and self.cancel.is_set():
                    return

                self.output.put(item)

                if self.cancel and self.cancel.is_set():
                    return
        finally:
            if self.should_end:
                self.output.end()


class generate_parallel(threading.Thread):
    """
    Run a generator on a multiple separate threads, placing the combined
    results in to a FiniteQueue. The queue is available via `instance.output`.

    Examples
    --------
    Create some random numbers on a separate thread.

    >>> def yield_numbers():
    >>>     for number in range(2)
    >>>         yield number
    >>>
    >>> for number in generate_parallel(yield_numbers, parallel=3).output:
    >>>     print(number)
    0
    0
    0
    1
    1
    1
    """

    def __init__(self, target, *args, parallel=5, **kwargs):
        super().__init__()

        self.output = FiniteQueue()
        kwargs['_output'] = self.output
        self.threads = [generate_on_thread(target, *args, **kwargs)
                        for _ in range(parallel)]

        if kwargs.get('immediate', True):
            self.start()

    def run(self):
        for thread in self.threads:
            thread.join()
        self.output.end()


class map_parallel(generate_parallel):
    """
    Like `map()`, but runs a function across items in an iterable on multiple
    parallel threads.
    """
    def __init__(self, target, input, *args, **kwargs):
        self.work_target = target
        self.input = input
        super().__init__(self.worker, *args, **kwargs)

    def worker(self, *args, **kwargs):
        for item in self.input:
            yield self.work_target(item, *args, **kwargs)


def tap(iterable, action):
    """
    Run a function against every item in an iterable and yield all the items
    from that iterable so it can continue to be used.
    """
    for item in iterable:
        action(item)
        yield item


BOUNDARY = re.compile(r'[\r\n\s.;:!?,<>{}[\]\-–—\|\\/]+')
IGNORABLE = re.compile('[\'‘’"“”]')
STOPWORDS = set(map(lambda word: IGNORABLE.sub('', word),
                    stopwords.words('english')))
STOPWORDS.add('&')


# FIXME: generate a single diff with -1, 0 and 1
class CharacterToWordDiffs:
    """
    Convert a character-by-character diff to a word-by-word diff. Resulting
    words are normalized -- they are all lowercase, punctuation is removed,
    etc. The tokenization here is also a little naive.
    """

    @classmethod
    def word_diffs(cls, text_changes):
        insertions = CharacterToWordDiffs(1)
        deletions = CharacterToWordDiffs(-1)

        for change in text_changes:
            if change[0] == 0 or change[0] == 1:
                insertions.add_text(change[1], change[0] != 0)

            if change[0] == 0 or change[0] == -1:
                deletions.add_text(change[1], change[0] != 0)

        insertions.add_text('', False)
        deletions.add_text('', False)

        return deletions.diff, insertions.diff

    def __init__(self, change_type):
        self.diff = []
        self.buffer = ''
        self.has_change = False
        self.change_type = change_type

    def add_text(self, text, is_change):
        remaining = text
        remaining = IGNORABLE.sub('', remaining)
        while True:
            boundary = BOUNDARY.search(remaining)
            if boundary is None:
                break

            if boundary.start() > 0:
                self.has_change = is_change or self.has_change
                self.buffer += remaining[:boundary.start()]
            self.complete_word()
            remaining = remaining[boundary.end():]

        if remaining:
            self.has_change = is_change or self.has_change
            self.buffer += remaining

        if text == '':
            self.complete_word()

    def complete_word(self):
        # TODO: get the stem instead of the word?
        # TODO: recognize `. ` as a sentence break and use it when n-gramming
        #       Probably similar things like em dashes, semicolons, commas
        word = self.buffer.lower()
        change_type = self.change_type if self.has_change else 0
        if word:
            self.diff.append((change_type, word))

        self.has_change = False
        self.buffer = ''


def changed_ngrams(diff, size=1):
    token_buffer = []
    change_buffer = []
    for item in diff:
        if item[1] in STOPWORDS:
            token_buffer.clear()
            change_buffer.clear()
        else:
            token_buffer.append(item[1])
            change_buffer.append(item[0])
            if len(token_buffer) == size:
                if any(change_buffer):
                    yield ' '.join(token_buffer)
                token_buffer.pop(0)
                change_buffer.pop(0)


def net_change(deletions, additions):
    """
    Helper for figuring out the overall change in usage of a set of terms.
    """
    net_count = Counter(additions)
    net_count.subtract(Counter(deletions))
    zeros = [key for key, value in net_count.items() if value == 0]
    for key in zeros:
        del net_count[key]

    return net_count


@retry(tries=3, delay=1)
def load_url(url, raise_status=True, timeout=5, **request_args):
    response = requests.get(url, timeout=timeout, **request_args)
    if raise_status and not response.ok:
        print(f'Raising on {url}')
        response.raise_for_status()

    content_type = response.headers.get('content-type', '')
    if 'charset=' not in content_type:
        response.encoding = 'utf-8'

    return response


@retry(tries=2, delay=1)
def load_url_readability(url):
    response = load_url(f'http://localhost:7323/proxy', params={'url': url},
                        timeout=45, raise_status=False)

    if response.status_code >= 500:
        try:
            data = response.json()
            if data['error'] == 'TIMEDOUT':
                # Throw in a little delay to give the server some time.
                time.sleep(10)
        except ValueError:
            pass
        response.raise_for_status()
    # Return None if the URL was unparseable.
    elif response.status_code >= 400:
        return None
    else:
        return response


def parallel(*calls):
    """Run several function calls in parallel threads."""
    calls = list(calls)
    with ThreadPoolExecutor(max_workers=len(calls)) as executor:
        tasks = [executor.submit(call, *args) for call, *args in calls]
        return [task.result() for task in tasks]


# TODO: backport this to web-monitoring-processing. It has added logic to clean
# up child processes before hard aborting.
class QuitSignal(Signal):
    """
    A context manager that handles system signals by triggering a
    `threading.Event` instance, giving your program an opportunity to clean up
    and shut down gracefully. If the signal is repeated a second time, the
    process quits immediately.

    Parameters
    ----------
    signals : int or tuple of int
        The signal or list of signals to handle.
    graceful_message : string, optional
        A message to print to stdout when a signal is received.
    final_message : string, optional
        A message to print to stdout before exiting the process when a repeat
        signal is received.

    Examples
    --------
    Quit on SIGINT (ctrl+c) or SIGTERM:

    >>> with QuitSignal((signal.SIGINT, signal.SIGTERM)) as cancel:
    >>>     for item in some_list:
    >>>         if cancel.is_set():
    >>>             break
    >>>         do_some_work()
    """
    def __init__(self, signals, graceful_message=None, final_message=None):
        self.event = threading.Event()
        self.graceful_message = graceful_message or (
            'Attempting to finish existing work before exiting. Press ctrl+c '
            'to stop immediately.')
        self.final_message = final_message or (
            'Stopping immediately and aborting all work!')
        super().__init__(signals, self.handle_interrupt)

    def handle_interrupt(self, signal_type, frame):
        if not self.event.is_set():
            print(self.graceful_message)
            self.event.set()
        else:
            # Clean up any child processes, otherwise they might be left alive.
            for child in multiprocessing.active_children():
                child.terminate()
            print(self.final_message)
            os._exit(100)

    def __enter__(self):
        super().__enter__()
        return self.event


class ActivityMonitor:
    """
    Track how long a block of code takes to run and log alerts if it takes
    longer than `alert_after` seconds. Continues alerting on every multiple of
    `alert_after` until the block completes.

    Useful for identifying operations that might be hanging or delaying a large
    data pipeline.

    Examples
    --------
    >>> with ActivityMonitor('some long operation'):
    >>>     do_something_that_takes_a_while()
    Waiting for some long operation (15 s)
    Waiting for some long operation (30 s)
    """
    def __init__(self, name, alert_after=15, output=sys.stderr):
        self.name = name
        self.wait_time = alert_after
        self.output = output
        self.cancel = threading.Event()
        self.start_time = time.time()
        self.watcher = threading.Thread(target=self.watch, daemon=True)
        self.watcher.start()

    def finish(self):
        self.cancel.set()

    def watch(self):
        time.sleep(self.wait_time)
        while not self.cancel.is_set():
            delta = round(time.time() - self.start_time)
            print(f'Waiting for {self.name} ({delta} s)', file=self.output)
            time.sleep(self.wait_time)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.finish()
