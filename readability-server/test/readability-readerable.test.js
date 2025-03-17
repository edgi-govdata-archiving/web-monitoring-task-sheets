/**
 * Test our fork of readability-readerable. This is based on Readibility's
 * existing tests at:
 * https://github.com/mozilla/readability/blob/master/test/test-isProbablyReaderable.js
 *
 * We want to make sure we tend to come up with similar results in our fork.
 *
 * WARNING: THIS IS BROKEN!
 * We used to pull in Readability via git, but now we use an actual NPM module
 * that they publish. However, it does not include all the test data used here.
 */
const { JSDOM } = require('jsdom');
const chai = require('chai');
const expect = chai.expect;

const testPages = require('@mozilla/readability/test/utils').getTestPages();
const Readable = require('../readability-readerable');

describe("isProbablyReaderable - upstream test pages", function() {
  const url = 'http://fakehost/test/page.html';
  console.log('Parsing test HTML pages; this may take a bit...');

  testPages.forEach(function(testPage) {
    // Parse the HTML outside the test so test timing only measures the
    // readable check (parsing takes *much* longer).
    const doc = new JSDOM(testPage.source, { url }).window.document;
    const expected = testPage.expectedMetadata.readerable;

    it(`${testPage.dir} should ${expected ? '' : 'not '}be readerable`, function() {
      expect(Readable.isProbablyReaderable(doc)).to.equal(expected);
    });
  });
});
