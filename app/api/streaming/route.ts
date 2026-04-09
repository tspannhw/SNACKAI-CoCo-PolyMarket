import { NextResponse } from 'next/server';
import { querySnowflake } from '@/lib/snowflake';

export const revalidate = 30;

export async function GET() {
  try {
    const [summary, health, totalRows] = await Promise.all([
      querySnowflake(`
        SELECT category, market_count, total_volume, total_24hr_volume,
               total_liquidity, avg_spread, last_updated
        FROM POLYMARKET.STREAMING.V_MARKET_VOLUME_SUMMARY
        ORDER BY total_volume DESC
        LIMIT 20
      `),
      querySnowflake(`
        SELECT minute_bucket, batches, total_markets,
               avg_duration_ms, max_duration_ms, error_count
        FROM POLYMARKET.STREAMING.V_INGESTION_HEALTH
        ORDER BY minute_bucket DESC
        LIMIT 60
      `),
      querySnowflake(`
        SELECT
          COUNT(*) AS total_markets,
          COUNT(DISTINCT batch_id) AS total_batches,
          SUM(volume_num) AS total_volume,
          SUM(volume_24hr) AS total_24hr_volume,
          SUM(liquidity_num) AS total_liquidity,
          MAX(ingested_at) AS last_ingested
        FROM POLYMARKET.STREAMING.V_LATEST_MARKETS
        WHERE active = TRUE
      `),
    ]);

    return NextResponse.json({
      summary,
      health,
      totals: totalRows[0] || {},
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    const msg = error instanceof Error ? error.message : String(error);
    const isNotConfigured = msg.includes('not configured');
    if (!isNotConfigured) console.error('Streaming API error:', error);
    return NextResponse.json(
      { error: isNotConfigured ? 'Snowflake not configured' : 'Failed to fetch streaming data', summary: [], health: [], totals: {} },
      { status: isNotConfigured ? 200 : 500 }
    );
  }
}
