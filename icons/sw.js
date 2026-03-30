// Piteraq Monitor — Service Worker
// Strategi: cache app-skallet (HTML/JS/CSS), la API-kall gå til nett.
// Øker aldri cachet API-data — varselet skal alltid være ferskt.

const CACHE_NAME = 'piteraq-v1';

const APP_SHELL = [
  '/Piteraq-Warning-v5/',
  '/Piteraq-Warning-v5/index.html',
  '/Piteraq-Warning-v5/manifest.json'
  // Legg til CSS- og JS-filer her hvis de er separate:
  // '/Piteraq-Warning-v5/style.css',
  // '/Piteraq-Warning-v5/app.js',
];

// API-domener som alltid skal gå rett til nett (aldri caches)
const API_HOSTS = [
  'api.open-meteo.com',
  'dmigw.govcloud.dk',
  'opendata.smhi.se'
];

// --- Install: cache app-skallet ---
self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then(cache => cache.addAll(APP_SHELL))
      .then(() => self.skipWaiting())
  );
});

// --- Activate: rydd opp gamle cacher ---
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(
        keys
          .filter(key => key !== CACHE_NAME)
          .map(key => caches.delete(key))
      )
    ).then(() => self.clients.claim())
  );
});

// --- Fetch: nett-først for API, cache-first for app-skall ---
self.addEventListener('fetch', event => {
  const url = new URL(event.request.url);

  // API-kall: alltid nett, aldri cache
  if (API_HOSTS.some(host => url.hostname.includes(host))) {
    event.respondWith(fetch(event.request));
    return;
  }

  // App-skall: cache-first, med nett som fallback
  event.respondWith(
    caches.match(event.request).then(cached => {
      return cached || fetch(event.request).then(response => {
        // Bare cache gyldige svar
        if (!response || response.status !== 200 || response.type !== 'basic') {
          return response;
        }
        const toCache = response.clone();
        caches.open(CACHE_NAME).then(cache => {
          cache.put(event.request, toCache);
        });
        return response;
      });
    })
  );
});
