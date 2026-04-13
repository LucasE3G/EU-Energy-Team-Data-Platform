# National Renovation Building Plans Visualizer

A web application to visualize data points from national renovation building plans across multiple European countries.

## Setup

### 1. Supabase Setup

1. Create a free account at [supabase.com](https://supabase.com)
2. Create a new project
3. Go to SQL Editor and run the SQL from `supabase_schema.sql`
4. Go to Project Settings > API and copy:
   - Project URL
   - `anon` `public` key (for frontend)
   - `service_role` `secret` key (for Python upload script)

### 2. Environment Configuration

1. Copy `.env.example` to `.env`
2. Fill in your Supabase credentials:
   ```
   SUPABASE_URL=https://your-project.supabase.co
   SUPABASE_ANON_KEY=your_anon_key
   SUPABASE_SERVICE_ROLE_KEY=your_service_role_key
   ```
3. Run the setup script to generate `config.js`:
   ```bash
   npm install
   npm run setup
   ```

### 3. Install Dependencies

**For Python upload script:**
```bash
pip install -r requirements.txt
```

**For Node.js frontend:**
```bash
npm install
```

### 4. Upload Data to Supabase

Run the Python script to upload all CSV files:
```bash
python upload_to_supabase.py
```

This will:
- Create country entries
- Upload all CSV data tables
- Upload measures data

### 5. Start Development Server

```bash
npm start
# (recommended) or
node server.js

# Static-only (no /api/* endpoints):
python -m http.server 3000
```

Then open `http://localhost:3000` in your browser.

## Project Structure

- `index.html` - Main HTML file
- `app.js` - Frontend JavaScript (Supabase client)
- `styles.css` - Styling
- `server.js` - Simple HTTP server
- `upload_to_supabase.py` - Script to upload CSVs to Supabase
- `supabase_schema.sql` - Database schema

## Data Structure

- **Countries**: Belgium, Bulgaria, Croatia, Finland, Lithuania, Romania, Slovenia, Spain
- **Data Tables**: Various CSV files with renovation data per country
- **Measures**: Policy measures and implementation plans per country
