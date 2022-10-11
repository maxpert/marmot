import { Hero } from "../components/Hero";
import Head from "next/head";

const Home = () => {
  return (
    <>
      <Head>
        <title>Marmot</title>
        <link rel="shortcut icon" href="/marmot/sillouhette-smooth.svg" type="image/x-icon" />
      </Head>
      <Hero />
    </>
  );
};

export default Home;
