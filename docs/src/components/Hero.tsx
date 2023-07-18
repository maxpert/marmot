import Link from "next/link";
import MemoMarmotLogo from "./Icons/MarmotLogo";

export const Hero = () => {
  return (
    <section className="flex items-center min-h-screen justify-center">
      <div className="max-w-full">
        <MemoMarmotLogo width="60rem" />
        <div className="text-center">
          {/* <p className="text-lg font-medium leading-8 text-indigo-600/95">Some text above</p> */}
          <h1 className="mt-3 text-[3.5rem] font-bold leading-[4rem] tracking-tight text-slate-400">
            Marmot
          </h1>
          <p className="mt-3 text-lg leading-relaxed text-slate-400">
            A distributed SQLite replicator
          </p>
        </div>

        <div className="mt-6 flex items-center justify-center gap-4">
          <a
            href="https://github.com/maxpert/marmot/releases/latest"
            target="_blank"
            rel="noopener noreferrer"
            className="transform rounded-md bg-marmot-blue-600 px-5 py-3 font-medium text-white transition-colors hover:bg-marmot-blue-800">
            Download latest
          </a>
          <Link href="/demo">
            <a className="transform rounded-md border bg-marmot-blue-200 border-slate-200 px-5 py-3 font-medium text-slate-900 transition-colors hover:bg-marmot-blue-400 hover:text-white">
              See it in action
            </a>
          </Link>
          <Link href="/intro">
            <a className="transform rounded-md border bg-marmot-blue-200 border-slate-200 px-5 py-3 font-medium text-slate-900 transition-colors hover:bg-marmot-blue-400 hover:text-white">
              Read the docs
            </a>
          </Link>
        </div>
      </div>
    </section>
  );
};
