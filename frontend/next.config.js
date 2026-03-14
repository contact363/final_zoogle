/** @type {import('next').NextConfig} */
const nextConfig = {
  // Required for Render Web Service deployment
  output: "standalone",

  images: {
    remotePatterns: [
      { protocol: "http", hostname: "**" },
      { protocol: "https", hostname: "**" },
    ],
  },

  // No rewrites needed — axios calls backend directly from browser.
  // CORS is configured on the backend to allow the frontend origin.
};

module.exports = nextConfig;
