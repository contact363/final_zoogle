import type { Metadata } from "next";
import { Inter } from "next/font/google";
import "./globals.css";
import { Toaster } from "react-hot-toast";

const inter = Inter({ subsets: ["latin"] });

export const metadata: Metadata = {
  title: "Zoogle – Industrial Machine Search",
  description: "Search 250,000+ industrial machines worldwide. CNC, Lathe, Laser Cutters, Injection Molding & more.",
  keywords: "CNC machines, industrial equipment, used machinery, laser cutting, injection molding",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className={`${inter.className} bg-steel-50 min-h-screen`}>
        {children}
        <Toaster position="top-right" />
      </body>
    </html>
  );
}
