'use client';

import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  LineChart,
  Line,
  PieChart,
  Pie,
  Cell,
} from 'recharts';
import { formatVolume } from '@/lib/validations';

const CHART_COLORS = [
  '#06b6d4', '#8b5cf6', '#f59e0b', '#10b981',
  '#ef4444', '#ec4899', '#3b82f6', '#14b8a6',
];

interface StatCardProps {
  title: string;
  value: string;
  subtitle?: string;
  trend?: 'up' | 'down' | 'neutral';
}

export function StatCard({ title, value, subtitle, trend }: StatCardProps) {
  const trendColor =
    trend === 'up' ? 'text-emerald-400' : trend === 'down' ? 'text-rose-400' : 'text-gray-400';

  return (
    <div className="bg-gray-900/80 border border-gray-700/50 rounded-lg p-4">
      <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-1">{title}</div>
      <div className="text-2xl font-mono font-bold text-gray-100">{value}</div>
      {subtitle && <div className={`text-xs mt-1 ${trendColor}`}>{subtitle}</div>}
    </div>
  );
}

interface VolumeChartProps {
  data: Array<{ CATEGORY?: string | null; TOTAL_VOLUME?: number | null; MARKET_COUNT?: number | null }>;
}

export function VolumeByCategoryChart({ data }: VolumeChartProps) {
  const chartData = data.slice(0, 10).map((d) => ({
    category: d.CATEGORY || 'Other',
    volume: d.TOTAL_VOLUME || 0,
    markets: d.MARKET_COUNT || 0,
  }));

  return (
    <div className="bg-gray-900/80 border border-gray-700/50 rounded-lg p-4">
      <h3 className="text-sm font-medium text-gray-200 mb-4">Volume by Category</h3>
      {chartData.length === 0 ? (
        <div className="flex items-center justify-center h-[280px] text-gray-500 text-xs">
          No volume data available. Start the streamer to load market data.
        </div>
      ) : (
      <ResponsiveContainer width="100%" height={280}>
        <BarChart data={chartData} margin={{ top: 5, right: 10, left: 10, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis dataKey="category" tick={{ fill: '#9ca3af', fontSize: 10 }} angle={-30} textAnchor="end" height={60} />
          <YAxis tick={{ fill: '#9ca3af', fontSize: 10 }} tickFormatter={(v) => formatVolume(v)} />
          <Tooltip
            contentStyle={{ background: '#1f2937', border: '1px solid #374151', borderRadius: 8, fontSize: 12 }}
            labelStyle={{ color: '#e5e7eb' }}
            formatter={(value: number) => [formatVolume(value), 'Volume']}
          />
          <Bar dataKey="volume" radius={[4, 4, 0, 0]}>
            {chartData.map((_, i) => (
              <Cell key={i} fill={CHART_COLORS[i % CHART_COLORS.length]} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
      )}
    </div>
  );
}

interface IngestionChartProps {
  data: Array<{
    MINUTE_BUCKET?: string | null;
    TOTAL_MARKETS?: number | null;
    AVG_DURATION_MS?: number | null;
    ERROR_COUNT?: number | null;
  }>;
}

export function IngestionHealthChart({ data }: IngestionChartProps) {
  const chartData = [...data]
    .reverse()
    .slice(-30)
    .map((d) => ({
      time: d.MINUTE_BUCKET ? new Date(d.MINUTE_BUCKET).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) : '',
      markets: d.TOTAL_MARKETS || 0,
      latency: Math.round(d.AVG_DURATION_MS || 0),
      errors: d.ERROR_COUNT || 0,
    }));

  return (
    <div className="bg-gray-900/80 border border-gray-700/50 rounded-lg p-4">
      <h3 className="text-sm font-medium text-gray-200 mb-4">Ingestion Health</h3>
      {chartData.length === 0 ? (
        <div className="flex items-center justify-center h-[280px] text-gray-500 text-xs text-center px-4">
          No ingestion data yet. Run <span className="font-mono bg-gray-800 px-1 rounded">./manage.sh stream</span> to start the pipeline and populate this chart.
        </div>
      ) : (
      <ResponsiveContainer width="100%" height={280}>
        <LineChart data={chartData} margin={{ top: 5, right: 10, left: 10, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis dataKey="time" tick={{ fill: '#9ca3af', fontSize: 10 }} />
          <YAxis tick={{ fill: '#9ca3af', fontSize: 10 }} />
          <Tooltip
            contentStyle={{ background: '#1f2937', border: '1px solid #374151', borderRadius: 8, fontSize: 12 }}
            labelStyle={{ color: '#e5e7eb' }}
          />
          <Line type="monotone" dataKey="markets" stroke="#06b6d4" strokeWidth={2} dot={false} name="Markets" />
          <Line type="monotone" dataKey="errors" stroke="#ef4444" strokeWidth={2} dot={false} name="Errors" />
        </LineChart>
      </ResponsiveContainer>
      )}
    </div>
  );
}

interface CategoryPieProps {
  data: Array<{ CATEGORY?: string | null; MARKET_COUNT?: number | null }>;
}

export function CategoryPieChart({ data }: CategoryPieProps) {
  const chartData = data.slice(0, 8).map((d) => ({
    name: d.CATEGORY || 'Other',
    value: d.MARKET_COUNT || 0,
  }));

  return (
    <div className="bg-gray-900/80 border border-gray-700/50 rounded-lg p-4">
      <h3 className="text-sm font-medium text-gray-200 mb-4">Markets by Category</h3>
      {chartData.length === 0 ? (
        <div className="flex items-center justify-center h-[280px] text-gray-500 text-xs">
          No category data available. Start the streamer to load market data.
        </div>
      ) : (
      <>
      <ResponsiveContainer width="100%" height={280}>
        <PieChart>
          <Pie
            data={chartData}
            cx="50%"
            cy="50%"
            innerRadius={60}
            outerRadius={100}
            paddingAngle={2}
            dataKey="value"
          >
            {chartData.map((_, i) => (
              <Cell key={i} fill={CHART_COLORS[i % CHART_COLORS.length]} />
            ))}
          </Pie>
          <Tooltip
            contentStyle={{ background: '#1f2937', border: '1px solid #374151', borderRadius: 8, fontSize: 12 }}
          />
        </PieChart>
      </ResponsiveContainer>
      <div className="flex flex-wrap gap-2 justify-center mt-2">
        {chartData.map((d, i) => (
          <div key={i} className="flex items-center gap-1 text-[10px] text-gray-400">
            <div className="w-2 h-2 rounded-full" style={{ background: CHART_COLORS[i % CHART_COLORS.length] }} />
            {d.name} ({d.value})
          </div>
        ))}
      </div>
      </>
      )}
    </div>
  );
}
