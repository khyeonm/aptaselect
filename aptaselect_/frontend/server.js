import express from 'express';
import path from 'path';
const app = express();

// Serve static files from the build directory
app.use(express.static('build'));

// Handle SvelteKit's client-side routing by serving index.html for all routes
let __dirname = path.resolve();
app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, 'build', 'index.html'));
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});
