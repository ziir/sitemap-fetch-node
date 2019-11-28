const { parse: parseURL, format: formatURL } = require('url');
const writeFileSync = require('fs').writeFileSync;
const https = require('https');
const parse = require('xml2js').parseString;

const { Agent, request } = https;

const SITEMAP_URL = 'https://www.gandi.net/sitemap.xml';
const CONCURRENT_FETCHES = 1;
const LIMIT = 0;
const RETRY = true;
const NOTIFY = true;

const errors = [];
const keepAliveAgent = new Agent({ keepAlive: true });
const fetch = (...args) =>
  new Promise((resolve, reject) => {
    request(...args, res => {
      let data = '';
      res.setEncoding('utf8');
      res.on('data', chunk => {
        data += chunk;
      });

      if (res.statusCode >= 400) {
        const url = typeof args[0] === 'object' ? formatURL(args[0]) : args[0];
        const error = { url, statusCode: res.statusCode };
        errors.push(error);
        console.error(`Oops: ${url} [${res.statusCode}]`);
      }
      res.on('end', () => resolve(data));
    })
      .on('error', reject)
      .end();
  });

const fetchXML = async (url) => {
  const xml = await fetch(url);
  return new Promise((resolve, reject) => {
    parse(xml, (err, result) => {
      if (err) return reject(err);
      resolve(result);
    });
  }).catch((err) => {
    console.error(`Unable to parse XML for ${url}`);
  });
};

const fetchDocument = url =>
  fetch({
    ...parseURL(url),
    agent: keepAliveAgent,
    headers: {
      'Accept-Encoding': 'gzip',
      'CDN-Country-Code': 'US',
      'User-Agent': 'sitemap-fetch-node',
    },
  });

const worker = (work, next_) => async (results) => {
  let next;
  while ((next = next_())) {
    const result = await work(next);
    if (results) results.push(result);
  }
};

const run = async () => {
  const start = new Date();

  console.log(`Fetching sitemap ...`);
  const {
    sitemapindex: { sitemap },
  } = await fetchXML(SITEMAP_URL);
  const locations = sitemap.map(({ loc }) => loc[0]);

  console.log(`Fetching ${locations.length} sub-sitemaps with ${CONCURRENT_FETCHES} workers.`);
  const sitemapsFetches = [];
  const urlsets = [];
  for (let i = 0; i < CONCURRENT_FETCHES; i++) {
    const w = worker(fetchXML, locations.pop.bind(locations));
    sitemapsFetches.push(w(urlsets));
  }
  await Promise.all(sitemapsFetches);

  let documents = []
    .concat(...urlsets.filter(Boolean).map(({ urlset: { url } }) => url))
    .map(obj => {
      const loc = obj.loc[0];
      const docs = [new URL(loc).href];
      const links = obj['xhtml:link'];
      if (Array.isArray(links)) {
        docs.push(
          ...links
            .map(({ $ }) => $.href !== loc && new URL($.href).href)
            .filter(Boolean),
        );
      }

      return docs;
    });

  documents = [].concat(...documents);
  for (let i = documents.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [documents[i], documents[j]] = [documents[j], documents[i]];
  }

  console.log(`Retrieved ${documents.length} documents URLs.`);

  const toFetch = [...new Set(documents.slice(0, LIMIT || documents.length))];
  console.log(
    `Fetching ${toFetch.length} documents with ${CONCURRENT_FETCHES} workers.`,
  );
  const AllFetches = [];
  for (let i = 0; i < CONCURRENT_FETCHES; i++) {
    const w = worker(fetchDocument, toFetch.pop.bind(toFetch));
    AllFetches.push(w());
  }

  await Promise.all(AllFetches);

  if (errors.length) {
    console.error('The following URLs fetch failed:');
    console.error(errors.map(({ url }) => url).join('\n'));
    if (RETRY) {
      console.log('Retrying failed fetches ...');
      await errors.reduce(async (acc, { url }) => {
        await acc;
        await fetchDocument(url);
      }, Promise.resolve());
    }
  }

  if (NOTIFY) {
    console.log('Notifying Google for re-crawl.');
    await fetch(`https://www.google.com/ping?sitemap=${SITEMAP_URL}`);
    console.log('Google notified.');
  }

  const end = new Date();
  console.log(`Finished in ${(end - start) / 1000}s !`);
};

run();
