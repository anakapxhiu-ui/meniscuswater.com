import type { Config } from "tailwindcss";

const config: Config = {
  content: ["./app/**/*.{ts,tsx}", "./components/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        // Meniscus brand: water, clarity, gravity
        ink: {
          DEFAULT: "#0A1628",
          muted: "#334155",
        },
        surface: {
          DEFAULT: "#FAFBFC",
          elevated: "#FFFFFF",
        },
        meniscus: {
          50: "#EFF8FB",
          100: "#D8EEF4",
          200: "#B2DDE9",
          300: "#7BC3D6",
          400: "#3EA1BD",
          500: "#1E7F9E",
          600: "#165F79",
          700: "#114A5F",
          800: "#0D3A4C",
        },
        severity: {
          0: "#94A3B8",
          1: "#10B981",
          2: "#F59E0B",
          3: "#EF4444",
          4: "#991B1B",
        },
      },
      fontFamily: {
        display: ["Fraunces", "Georgia", "serif"],
        body: ["Inter", "system-ui", "sans-serif"],
      },
    },
  },
  plugins: [],
};
export default config;
