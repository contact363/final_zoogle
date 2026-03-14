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

  async rewrites() {
    // Evaluated at Next.js server STARTUP (runtime), not baked into client bundle.
    // Hardcoded production URL as fallback so this works even if env var is missing.
    const backendUrl =
      process.env.NEXT_PUBLIC_API_URL ||
      "https://final-zoogle-backend.onrender.com";
    return [
      {
        source: "/api/:path*",
        destination: `${backendUrl}/api/:path*`,
      },
    ];
  },
};

module.exports = nextConfig;
