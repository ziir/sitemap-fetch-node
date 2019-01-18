const { parse: parseURL, format: formatURL } = require('url');
const writeFileSync = require('fs').writeFileSync;
const https = require('https');
const parse = require('xml2js').parseString;

const { Agent, request } = https;

const SITEMAP_URL = 'https://www.gandi.net/sitemap.xml';
const CONCURRENT_FETCHES = 200;
const LIMIT = 0;
const RETRY = true;
const NOTIFY = false;

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

const fetchXML = async (...args) => {
  const xml = await fetch(...args);
  return new Promise((resolve, reject) => {
    parse(xml, (err, result) => {
      if (err) return reject(err);
      resolve(result);
    });
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

const run = async () => {
  const start = new Date();

  console.log(`Fetching sitemap ...`);
  const {
    sitemapindex: { sitemap },
  } = await fetchXML(SITEMAP_URL);
  const locations = sitemap.map(({ loc }) => loc[0]);

  console.log(`Fetching ${locations.length} sub-sitemaps ...`);
  const urlsets = await Promise.all(
    locations.map(location => fetchXML(location).catch(() => {})),
  );

  const documents = []
    .concat(...urlsets.filter(Boolean).map(({ urlset: { url } }) => url))
    .map(({ loc }) => new URL(loc[0]).href);
  console.log(`Retrieved ${documents.length} documents URLs.`);

  const AllFetches = documents
    .slice(0, LIMIT || documents.length)
    .map(document => () => fetchDocument(document));
  const fetches = [];
  while (AllFetches.length) {
    fetches.push(AllFetches.splice(0, CONCURRENT_FETCHES));
  }

  console.log(
    `Fetching ${documents.length} documents in ${
      fetches.length
    } groups with ${CONCURRENT_FETCHES} concurrent fetches.`,
  );
  const fetchTimings = await fetches.reduce(async (acc, grouped, idx) => {
    const timings = await acc;
    const start = new Date();
    await Promise.all(grouped.map(f => f()));
    const end = new Date();
    const duration = end - start;
    console.log(`Fetched group ${idx + 1}/${fetches.length} in ${duration}ms`);
    return [duration].concat(timings);
  }, Promise.resolve([]));

  const average =
    fetchTimings.reduce((acc, current) => acc + current, 0) / fetches.length;
  console.log(`Group fetch average duration: ${average}ms`);
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
