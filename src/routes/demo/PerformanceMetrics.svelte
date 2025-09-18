<script lang="ts">
	import type { PerformanceMetrics } from './types.js';

	export let traditionalMetrics: PerformanceMetrics;
	export let ivmMetrics: PerformanceMetrics;

	$: speedupFactor =
		traditionalMetrics.updateTime > 0 ? traditionalMetrics.updateTime / ivmMetrics.updateTime : 1;

	$: memoryReduction =
		traditionalMetrics.totalMemory > 0
			? ((traditionalMetrics.totalMemory - ivmMetrics.totalMemory) /
					traditionalMetrics.totalMemory) *
				100
			: 0;

	function formatDuration(ms: number): string {
		if (ms < 1) return `${(ms * 1000).toFixed(1)}μs`;
		if (ms < 1000) return `${ms.toFixed(2)}ms`;
		return `${(ms / 1000).toFixed(2)}s`;
	}

	function formatPercentage(percent: number): string {
		return `${percent > 0 ? '+' : ''}${percent.toFixed(1)}%`;
	}
</script>

<div class="performance-comparison">
	<h2>Performance Comparison</h2>

	<div class="metrics-grid">
		<div class="metric-card">
			<h3>Update Time Comparison</h3>
			<div class="comparison">
				<div class="traditional">
					<span class="label">Traditional:</span>
					<span class="value">{formatDuration(traditionalMetrics.updateTime)}</span>
				</div>
				<div class="ivm">
					<span class="label">IVM:</span>
					<span class="value">{formatDuration(ivmMetrics.updateTime)}</span>
				</div>
			</div>
			<div class="speedup">
				<strong>{speedupFactor.toFixed(1)}x faster</strong>
			</div>
		</div>

		<div class="metric-card">
			<h3>Memory Usage Comparison</h3>
			<div class="comparison">
				<div class="traditional">
					<span class="label">Traditional:</span>
					<span class="value">{(traditionalMetrics.totalMemory / 1024).toFixed(1)} KB</span>
				</div>
				<div class="ivm">
					<span class="label">IVM:</span>
					<span class="value">{(ivmMetrics.totalMemory / 1024).toFixed(1)} KB</span>
				</div>
			</div>
			<div class="reduction">
				<strong>{formatPercentage(-memoryReduction)} reduction</strong>
			</div>
		</div>

		<div class="metric-card advantages">
			<h3>IVM Advantages</h3>
			<ul>
				<li>✅ Processes only incremental changes</li>
				<li>✅ Maintains materialized view state</li>
				<li>✅ Constant time updates (O(|Δ|) vs O(|D|))</li>
				<li>✅ Scalable to large datasets</li>
				<li>✅ Real-time query results</li>
			</ul>
		</div>

		<div class="metric-card">
			<h3>Algorithm Details</h3>
			<div class="algorithm-info">
				<p><strong>Traditional:</strong> Re-executes full query Q(D) on complete dataset D</p>
				<p><strong>IVM:</strong> Applies Q^Δ = D ∘ Q ∘ I to process only changes Δ</p>
			</div>
			<div class="complexity">
				<div>Traditional: O(|D|) per update</div>
				<div>IVM: O(|Δ|) per update</div>
			</div>
		</div>
	</div>
</div>

<style>
	.performance-comparison {
		background: white;
		border-radius: 12px;
		padding: 25px;
		box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
		border: 1px solid #e5e7eb;
	}

	.performance-comparison h2 {
		text-align: center;
		color: #1f2937;
		margin-bottom: 25px;
		font-size: 1.5rem;
	}

	.metrics-grid {
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
		gap: 20px;
	}

	.metric-card {
		background: #f9fafb;
		border-radius: 8px;
		padding: 20px;
		border: 1px solid #e5e7eb;
	}

	.metric-card h3 {
		margin: 0 0 15px 0;
		color: #374151;
		font-size: 1.1rem;
	}

	.comparison {
		display: flex;
		flex-direction: column;
		gap: 10px;
		margin-bottom: 15px;
	}

	.traditional,
	.ivm {
		display: flex;
		justify-content: space-between;
		align-items: center;
		padding: 8px 12px;
		border-radius: 6px;
	}

	.traditional {
		background: #fef2f2;
		border: 1px solid #fecaca;
	}

	.ivm {
		background: #f0fdf4;
		border: 1px solid #bbf7d0;
	}

	.label {
		color: #6b7280;
		font-weight: 500;
	}

	.value {
		font-family: 'Monaco', monospace;
		font-weight: 600;
	}

	.traditional .value {
		color: #dc2626;
	}

	.ivm .value {
		color: #059669;
	}

	.speedup,
	.reduction {
		text-align: center;
		padding: 10px;
		border-radius: 6px;
		background: #ecfdf5;
		color: #059669;
		border: 1px solid #bbf7d0;
	}

	.advantages {
		background: #f0f9ff;
		border: 1px solid #bae6fd;
	}

	.advantages ul {
		margin: 0;
		padding: 0 0 0 20px;
		list-style: none;
	}

	.advantages li {
		margin: 8px 0;
		color: #0369a1;
		font-size: 0.9rem;
	}

	.algorithm-info {
		font-size: 0.9rem;
		color: #6b7280;
		margin-bottom: 15px;
	}

	.algorithm-info p {
		margin: 8px 0;
		line-height: 1.5;
	}

	.complexity {
		background: #f3f4f6;
		border-radius: 6px;
		padding: 12px;
		font-family: 'Monaco', monospace;
		font-size: 0.85rem;
		color: #374151;
	}

	.complexity div {
		margin: 4px 0;
	}

	@media (max-width: 768px) {
		.metrics-grid {
			grid-template-columns: 1fr;
		}
	}
</style>
