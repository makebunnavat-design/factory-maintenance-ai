// Service Worker for Elin AI Repair Chatbot
// Simple caching strategy for offline functionality

const CACHE_PREFIX = 'elin-ai-';

// Detect proxy path
const isProxyPath = /^\/ai(?:\/|$)/.test(self.location.pathname || '');
const appBasePath = isProxyPath ? '/ai' : '';
const staticBasePath = isProxyPath ? '/ai/ai-static' : '/static';
const CACHE_NAME = `${CACHE_PREFIX}${isProxyPath ? 'proxy' : 'root'}-v3`;

const urlsToCache = [
  appBasePath + '/',
  staticBasePath + '/css/style.css',
  staticBasePath + '/js/app.js',
  staticBasePath + '/js/phaser.min.js',
  staticBasePath + '/js/tailwindcss.js',
  staticBasePath + '/js/chart.umd.min.js',
  staticBasePath + '/js/echarts.min.js',
  staticBasePath + '/Elin.mp3'
];

// Install event - cache resources
self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then((cache) => {
        console.log('Opened cache');
        return cache.addAll(urlsToCache);
      })
      .catch((error) => {
        console.log('Cache install failed:', error);
      })
  );
  self.skipWaiting();
});

// Fetch event - serve from cache when offline
self.addEventListener('fetch', (event) => {
  event.respondWith(
    caches.match(event.request)
      .then((response) => {
        // Return cached version or fetch from network
        if (response) {
          return response;
        }
        
        return fetch(event.request).catch((error) => {
          console.warn('Fetch failed for:', event.request.url, error);
          
          // If both cache and network fail, return a fallback
          if (event.request.destination === 'document') {
            return caches.match(appBasePath + '/');
          }
          
          // For other resources, return a 404 response
          return new Response('Resource not found', { 
            status: 404, 
            statusText: 'Not Found' 
          });
        });
      })
  );
});

// Activate event - clean up old caches
self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((cacheNames) => {
      return Promise.all(
        cacheNames.map((cacheName) => {
          if (cacheName.startsWith(CACHE_PREFIX) && cacheName !== CACHE_NAME) {
            console.log('Deleting old cache:', cacheName);
            return caches.delete(cacheName);
          }
        })
      );
    })
  );
  self.clients.claim();
});
