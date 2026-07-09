const express = require('express');

const app = express();
const port = process.env.PORT || 8080;

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', service: 'gateway' });
});

app.get('/api/overview', (_req, res) => {
  res.json({
    message: 'CRM platform gateway is running',
    services: ['bot-service', 'assignment-service', 'agent-service', 'notification-service']
  });
});

app.listen(port, () => {
  console.log(`Gateway listening on port ${port}`);
});
