'use client';

import { useEffect, useState, useCallback } from 'react';
import { StatCard } from './components/charts';
import { VolumeByCategoryChart, IngestionHealthChart, CategoryPieChart } from './components/charts';
import { MarketCard, MarketTable } from './components/market-cards';
import { formatCurrency, formatNumber, timeAgo } from '@/lib/utils';

interface DashboardData {
  markets: Array<Record<string, unknown>>;
  streaming: {
    summary: Array<Record<string, unknown>>;
    health: Array<Record<string, unknown>>;
    totals: Record<string, unknown>;
  };
}

type ViewMode = 'cards' | 'table';

export default function Dashboard() {
  const [data, setData] = useState<DashboardData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [viewMode, setViewMode] = useState<ViewMode>('cards');
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [lastUpdated, setLastUpdated] = useState<string>('');

  const fetchData = useCallback(async () => {
    try {
      const [marketsRes, streamingRes] = await Promise.all([
        fetch('/api/markets?limit=500&active=true'),
        fetch('/api/streaming'),
      ]);

      const marketsData = await marketsRes.json();
      const streamingData = await streamingRes.json();

      setData({
        markets: marketsData.markets || [],
        streaming: {
          summary: streamingData.summary || [],
          health: streamingData.health || [],
          totals: streamingData.totals || {},
        },
      });
      setLastUpdated(new Date().toISOString());
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch data');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  useEffect(() => {
    if (!autoRefresh) return;
    const interval = setInterval(fetchData, 15000);
    return () => clearInterval(interval);
  }, [autoRefresh, fetchData]);

  const handleExport = async () => {
    if (!data?.markets) return;
    try {
      const res = await fetch('/api/export', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          data: data.markets,
          filename: `polymarket_${new Date().toISOString().slice(0, 10)}`,
          format: 'csv',
        }),
      });
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `polymarket_${new Date().toISOString().slice(0, 10)}.csv`;
      a.click();
      URL.revokeObjectURL(url);
    } catch {
      console.error('Export failed');
    }
  };

  const totals = data?.streaming?.totals || {};

  // Determine pipeline status based on last ingestion timestamp
  const lastIngested = (totals as Record<string, string>).LAST_INGESTED;
  const pipelineStatus = (() => {
    if (!lastIngested) return 'offline' as const;
    const ageMs = Date.now() - new Date(lastIngested).getTime();
    const ageMinutes = ageMs / 60000;
    if (ageMinutes < 3) return 'healthy' as const;
    if (ageMinutes < 10) return 'stale' as const;
    return 'offline' as const;
  })();

  const pipelineStatusConfig = {
    healthy: { label: 'Pipeline Active', dotClass: 'bg-emerald-400', textClass: 'text-emerald-400' },
    stale:   { label: 'Pipeline Stale',  dotClass: 'bg-yellow-400',  textClass: 'text-yellow-400' },
    offline: { label: 'Pipeline Offline', dotClass: 'bg-rose-400',    textClass: 'text-rose-400' },
  };
  const statusInfo = pipelineStatusConfig[pipelineStatus];

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <div className="text-center">
          <div className="text-cyan-400 text-lg mb-2">Loading Dashboard...</div>
          <div className="text-gray-500 text-xs">Connecting to Snowflake</div>
        </div>
      </div>
    );
  }

  return (
    <main className="min-h-screen p-4 max-w-[1440px] mx-auto">
      {/* Header */}
      <header className="flex items-center justify-between mb-6">
        <div>
          <div className="flex items-center gap-3">
            <h1 className="text-xl font-bold text-gray-100">
              Polymarket Streaming Dashboard
            </h1>
            <div className="flex items-center gap-1.5">
              <div className={`w-2 h-2 rounded-full ${statusInfo.dotClass} ${pipelineStatus === 'healthy' ? 'animate-pulse' : ''}`} />
              <span className={`text-[10px] font-medium ${statusInfo.textClass}`}>{statusInfo.label}</span>
            </div>
          </div>
          <p className="text-xs text-gray-500 mt-0.5">
            Snowpipe Streaming v2 High-Performance | {timeAgo(lastUpdated)} | {formatNumber(data?.markets?.length || 0)} markets
          </p>
        </div>
        <div className="flex items-center gap-3">
          <label className="flex items-center gap-2 text-xs text-gray-400 cursor-pointer">
            <input
              type="checkbox"
              checked={autoRefresh}
              onChange={(e) => setAutoRefresh(e.target.checked)}
              className="rounded"
            />
            Auto-refresh (15s)
          </label>
          <div className="flex bg-gray-800 rounded overflow-hidden">
            <button
              onClick={() => setViewMode('cards')}
              className={`px-3 py-1.5 text-xs ${viewMode === 'cards' ? 'bg-cyan-600 text-white' : 'text-gray-400'}`}
            >
              Cards
            </button>
            <button
              onClick={() => setViewMode('table')}
              className={`px-3 py-1.5 text-xs ${viewMode === 'table' ? 'bg-cyan-600 text-white' : 'text-gray-400'}`}
            >
              Table
            </button>
          </div>
          <button
            onClick={fetchData}
            className="px-3 py-1.5 text-xs bg-gray-800 text-gray-300 rounded hover:bg-gray-700 transition-colors"
          >
            Refresh
          </button>
        </div>
      </header>

      {error && (
        <div className="bg-rose-900/30 border border-rose-700/50 rounded-lg p-3 mb-4 text-xs text-rose-300">
          {error}
        </div>
      )}

      {pipelineStatus === 'stale' && (
        <div className="bg-yellow-900/30 border border-yellow-700/50 rounded-lg p-3 mb-4 text-xs text-yellow-300">
          Pipeline data is stale (last ingestion: {timeAgo(lastIngested)}). The Python streamer may have stopped. Run <code className="bg-yellow-900/50 px-1 rounded">./manage.sh stream</code> to restart.
        </div>
      )}

      {pipelineStatus === 'offline' && (
        <div className="bg-rose-900/30 border border-rose-700/50 rounded-lg p-3 mb-4 text-xs text-rose-300">
          No recent ingestion data detected. Start the streamer with <code className="bg-rose-900/50 px-1 rounded">./manage.sh stream</code> to populate the dashboard.
        </div>
      )}

      {/* KPI Stats */}
      <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-3 mb-6">
        <StatCard
          title="Active Markets"
          value={formatNumber((totals as Record<string, number>).TOTAL_MARKETS || data?.markets?.length || 0)}
        />
        <StatCard
          title="Total Volume"
          value={formatCurrency((totals as Record<string, number>).TOTAL_VOLUME || 0)}
        />
        <StatCard
          title="24h Volume"
          value={formatCurrency((totals as Record<string, number>).TOTAL_24HR_VOLUME || 0)}
        />
        <StatCard
          title="Total Liquidity"
          value={formatCurrency((totals as Record<string, number>).TOTAL_LIQUIDITY || 0)}
        />
        <StatCard
          title="Batches"
          value={formatNumber((totals as Record<string, number>).TOTAL_BATCHES || 0)}
        />
        <StatCard
          title="Last Ingested"
          value={timeAgo((totals as Record<string, string>).LAST_INGESTED)}
        />
      </div>

      {/* Charts Row */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4 mb-6">
        <VolumeByCategoryChart data={(data?.streaming?.summary || []) as Array<{ CATEGORY?: string | null; TOTAL_VOLUME?: number | null; MARKET_COUNT?: number | null }>} />
        <IngestionHealthChart data={(data?.streaming?.health || []) as Array<{ MINUTE_BUCKET?: string | null; TOTAL_MARKETS?: number | null; AVG_DURATION_MS?: number | null; ERROR_COUNT?: number | null }>} />
        <CategoryPieChart data={(data?.streaming?.summary || []) as Array<{ CATEGORY?: string | null; MARKET_COUNT?: number | null }>} />
      </div>

      {/* Markets */}
      {viewMode === 'cards' ? (
        <div>
          <h2 className="text-sm font-medium text-gray-200 mb-3">
            Top Markets by Volume
          </h2>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
            {(data?.markets || []).slice(0, 60).map((market, i) => (
              <MarketCard key={(market as Record<string, string>).ID || i} market={market as Record<string, unknown> & { ID?: string | null }} />
            ))}
          </div>
        </div>
      ) : (
        <MarketTable
          markets={(data?.markets || []) as Array<Record<string, unknown> & { ID?: string | null }>}
          onExport={handleExport}
        />
      )}

      {/* Footer */}
      <footer className="mt-8 pt-4 border-t border-gray-800 text-center text-[10px] text-gray-600">
        Polymarket Prediction Markets | Snowpipe Streaming v2 High-Performance REST API | Snowflake
      </footer>
    </main>
  );
}
