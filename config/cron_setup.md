# Cron Setup

## Delivery target
- Channel: `telegram`
- Destination: `-1003845783711:topic:7`
- Timezone: `America/Chicago`

## Jobs
1. Main trading pipeline
2. Trade alert dispatch
3. Morning daily summary

## Verification commands
```bash
openclaw cron status
openclaw cron list
openclaw cron runs --limit 20
```
