/**
 * Setup script to load Supabase config from environment variables
 * Run this with: node setup-config.js
 * This will create a config.js file with your Supabase credentials
 */

const fs = require('fs');
const path = require('path');
require('dotenv').config();

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY;

if (!SUPABASE_URL || !SUPABASE_ANON_KEY) {
    console.error('❌ Error: SUPABASE_URL and SUPABASE_ANON_KEY must be set in .env file');
    console.log('\nPlease create a .env file with:');
    console.log('SUPABASE_URL=your_supabase_url');
    console.log('SUPABASE_ANON_KEY=your_anon_key');
    process.exit(1);
}

const configContent = `// Supabase Configuration
// Auto-generated from .env file
const SUPABASE_CONFIG = {
    url: '${SUPABASE_URL}',
    anonKey: '${SUPABASE_ANON_KEY}'
};
`;

fs.writeFileSync(path.join(__dirname, 'config.js'), configContent);
console.log('✅ Config file created successfully!');
console.log('📝 Supabase URL:', SUPABASE_URL.substring(0, 30) + '...');
