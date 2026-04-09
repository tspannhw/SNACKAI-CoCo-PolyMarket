import { NextResponse } from 'next/server';
import { querySnowflake } from '@/lib/snowflake';

export const revalidate = 15;

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const limit = Math.min(Number(searchParams.get('limit') || 500), 1000);
  const category = searchParams.get('category');
  const activeOnly = searchParams.get('active') !== 'false';

  try {
    let sql = `
      SELECT
        id, question, category, outcome_prices, outcomes,
        volume_num, volume_24hr, volume_1wk,
        liquidity_num, spread, active, closed, end_date,
        market_type, image, ingested_at, batch_id, score
      FROM POLYMARKET.STREAMING.V_LATEST_MARKETS
      WHERE 1=1
    `;

    if (activeOnly) sql += ` AND active = TRUE AND closed = FALSE`;
    if (category) sql += ` AND category = '${category.replace(/'/g, "''")}'`;
    sql += ` ORDER BY volume_num DESC NULLS LAST LIMIT ${limit}`;

    const rows = await querySnowflake(sql);

    return NextResponse.json({
      markets: rows,
      count: rows.length,
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    const msg = error instanceof Error ? error.message : String(error);
    const isNotConfigured = msg.includes('not configured');
    if (!isNotConfigured) console.error('Markets API error:', error);
    return NextResponse.json(
      { error: isNotConfigured ? 'Snowflake not configured' : 'Failed to fetch markets', markets: [], count: 0 },
      { status: isNotConfigured ? 200 : 500 }
    );
  }
}
