const basePath = "/marmot";

/**
 * @type {import("nextra-theme-docs").DocsThemeConfig}
 */
export default {
  project: { link: "https://github.com/maxpert/marmot" },
  docsRepositoryBase: "https://github.com/maxpert/marmot/tree/master/docs",
  titleSuffix: " - Marmot V2",
  logo: (
    <>
      <img width={50} style={{ marginRight: 5 }} src={`${basePath}/logo.png`} />
      <span className="text-gray-600 font-normal hidden md:inline">
        A distributed SQLite replicator built on a gossip-based protocol
      </span>
    </>
  ),
  head: (
    <>
      <meta name="msapplication-TileColor" content="#ffffff" />
      <meta name="theme-color" content="#ffffff" />
      <meta name="viewport" content="width=device-width, initial-scale=1.0" />
      <meta httpEquiv="Content-Language" content="en" />
      <meta
        name="description"
        content="Marmot: A distributed SQLite replicator built on a gossip-based protocol"
      />
      <meta
        name="og:description"
        content="Marmot: A distributed SQLite replicator built on a gossip-based protocol"
      />
      <meta name="twitter:card" content="summary_large_image" />
      <meta name="twitter:image" content={`${basePath}/logo.png`} />
      <meta name="twitter:site:domain" content="https://github.com/maxpert/marmot" />
      <meta name="twitter:url" content="https://github.com/maxpert/marmot" />
      <meta
        name="og:title"
        content="Marmot: A distributed SQLite replicator built on a gossip-based protocol"
      />
      <meta name="og:image" content={`${basePath}/logo.png`} />
      <meta name="apple-mobile-web-app-title" content="Marmot" />
      <link rel="apple-touch-icon" sizes="180x180" href={`${basePath}/logo.png`} />
      <link rel="icon" type="image/png" sizes="192x192" href={`${basePath}/logo.png`} />
      <link rel="icon" type="image/png" sizes="32x32" href={`${basePath}/logo.png`} />
      <link rel="icon" type="image/png" sizes="96x96" href={`${basePath}/logo.png`} />
      <link rel="icon" type="image/png" sizes="16x16" href={`${basePath}/logo.png`} />
      <meta name="msapplication-TileImage" content="/ms-icon-144x144.png" />
    </>
  ),
  navigation: true,
  footer: { text: <>MIT {new Date().getFullYear()} © Marmot.</> },
  editLink: { text: "Edit this page on GitHub" },
  unstable_faviconGlyph: "👋",
};
