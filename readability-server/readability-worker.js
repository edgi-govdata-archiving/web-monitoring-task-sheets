const { JSDOM } = require('jsdom');
const { Readability } = require('@mozilla/readability');
const WorkerPool = require('./worker-pool');
const Readable = require('./readability-readerable');

WorkerPool.implementWorker((html, url, force = false) => {
  // XXX: Mega-selects really slow things down and we'll ultimately strip them
  // later anyway, so do it now to increase parsing speed.
  html = html.replace(/<select.+?<\/select>/gs, (match) => {
    const options = match.match(/<option/g);
    if (options && options.length > 10) return '';
    return match;
  });

  const dom = new JSDOM(html, { url });

  // isProbablyReaderable tries to check whether there is a sizeable text body
  // actually worth extracting. (You might have a page that can technically
  // parse, but that doesn't have meaningful content to actually get, and
  // parsing it likely leaves you with gibberish.)
  if (!force && !Readable.isProbablyReaderable(dom.window.document)) {
    return null;
  }

  // XXX: Can we do this better?????
  dom.window.document.querySelectorAll('[class*="teaser"],[class*="related"]')
    .forEach(node => node.setAttribute('hidden', 'hidden'))
  // // XXX: Mega-selects really slow things down!
  // // e.g. http://www.ndbc.noaa.gov/station_history.php?station=48917
  // dom.window.document.querySelectorAll('select').forEach(node => {
  //   if (node.childElementCount > 10) node.remove();
  // });

  const reader = new Readability(dom.window.document);
  const article = reader.parse();

  if (article) {
    return {
      text: `${article.title}\n\n${article.textContent}`,
      html: article.content,
      // Readability modifies the DOM, so we can get the non-content bits by
      // just serializing the DOM that's left behind.
      nonContentHtml: dom.serialize(),
      // TODO: Get non-content text? Needs fancier logic than dom.textContent.
    }
  }
  else {
    return null;
  }
});
