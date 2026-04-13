export default function handler(req, res) {
  const url = process.env.SUPABASE_URL;
  const anonKey = process.env.SUPABASE_ANON_KEY;

  if (!url || !anonKey) {
    return res.status(500).json({
      error: "missing_config",
      message: "Missing SUPABASE_URL or SUPABASE_ANON_KEY",
    });
  }

  // These are safe to expose to the browser (anon key is public by design).
  return res.status(200).json({ url, anonKey });
}

