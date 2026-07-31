const http = require('http');

const PORT = process.env.PORT || 3000;

const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', service: 'godezk-device-vision-backend' }));
  } else {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('GoDezk Device Vision Backend\n');
  }
});

server.listen(PORT, () => {
  console.log(`Backend server listening on port ${PORT}`);
});
