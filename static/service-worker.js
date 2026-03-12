const CACHE_NAME = "train-watch-v3";
const ASSETS = [
  "/",
  "/manifest.webmanifest",
  "/service-worker.js",
  "/static/styles.css",
  "/static/app.js",
  "/static/icon.svg",
];

self.addEventListener("install", (event) => {
  self.skipWaiting();
  event.waitUntil(caches.open(CACHE_NAME).then((cache) => cache.addAll(ASSETS)));
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((key) => key !== CACHE_NAME).map((key) => caches.delete(key)))
    ).then(() => self.clients.claim())
  );
});

async function networkFirst(request) {
  try {
    const response = await fetch(request);
    const cache = await caches.open(CACHE_NAME);
    cache.put(request, response.clone());
    return response;
  } catch (_error) {
    const cached = await caches.match(request);
    if (cached) return cached;
    return caches.match("/");
  }
}

async function staleWhileRevalidate(request) {
  const cache = await caches.open(CACHE_NAME);
  const cached = await cache.match(request);
  const networkPromise = fetch(request)
    .then((response) => {
      cache.put(request, response.clone());
      return response;
    })
    .catch(() => null);
  return cached || networkPromise || caches.match("/");
}

self.addEventListener("fetch", (event) => {
  if (event.request.method !== "GET") return;
  const url = new URL(event.request.url);

  if (event.request.mode === "navigate" || url.pathname === "/") {
    event.respondWith(networkFirst(event.request));
    return;
  }

  if (ASSETS.includes(url.pathname)) {
    event.respondWith(staleWhileRevalidate(event.request));
    return;
  }

  event.respondWith(fetch(event.request).catch(() => caches.match(event.request)));
});
