/* eslint-disable @typescript-eslint/no-var-requires */
/**
 * @type {import('next').NextConfig}
 */
const nextConfig = {
  reactStrictMode: true,
  basePath: "/marmot",
  output: "export",
  images: {
    unoptimized: true,
  },
};

const withNextra = require("nextra")({
  theme: "nextra-theme-docs",
  themeConfig: "./theme.config.js",
  // unstable_staticImage: true,
});
module.exports = withNextra(nextConfig);
