# How to open the app on localhost (without Node.js)

This project's server only serves static files (HTML, JS, CSS). You can use any of these:

---

## Option 1: Python (if you have Python installed)

In a terminal, from this folder (`app`), run:

```powershell
python -m http.server 3000
```

Then open in your browser: **http://localhost:3000**

To stop: press `Ctrl+C` in the terminal.

---

## Option 2: Live Server in Cursor / VS Code

1. Install the **Live Server** extension (by Ritwick Dey).
2. Right-click `index.html` in the file explorer.
3. Click **"Open with Live Server"**.

The app will open in your browser. Default URL is often http://127.0.0.1:5500 (port may vary).

---

## Option 3: PHP (if you have PHP installed)

```powershell
php -S localhost:3000
```

Then open: **http://localhost:3000**

---

**Note:** This app uses **Node.js** (JavaScript), not Java. If you can install Node.js from https://nodejs.org later, you can use `npm start` instead.
