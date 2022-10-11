/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ["./src/pages/**/*.{js,ts,jsx,tsx,mdx}", "./src/components/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        "marmot-blue": {
          100: "#698EA6",
          200: "#5384A3",
          300: "#437A9D",
          400: "#327199",
          500: "#226997",
          600: "#126397",
          700: "#015D98",
          800: "#0E517B",
          900: "#174765",
          DEFAULT: "#126397",
        },
      },
    },
  },
  plugins: [],
};
