## IMPORTANT INFORMATION

Discord webhook URL:
https://discord.com/api/webhooks/1345043333512433704/sEs2k8ZZZl09xpG0yRMdeBm2gzYyFW3h2GF7cNnxWiMIArBabgJwj6hwOkFrh3LpmPd5

Post call with curl:

```bash
curl -X POST "http://localhost/rules" -H "Content-Type: application/json" -d '{
  "metric_name": "packages-received",
  "threshold": 500,
  "discord_webhook_url": "https://discord.com/api/webhooks/1345043333512433704/sEs2k8ZZZl09xpG0yRMdeBm2gzYyFW3h2GF7cNnxWiMIArBabgJwj6hwOkFrh3LpmPd5"
}'
```
