import { Stream } from './stream.js';
import { ZSet } from './z-set.js';
import { Circuit } from './circuit.js';
import { IncrementalViewMaintenance } from './incremental-view-maintenance.js';

export interface QueryOperation {
	type: 'source' | 'filter' | 'project' | 'join' | 'distinct' | 'union';

	// Source operation
	stream?: Stream;

	// Filter operation
	predicate?: (item: any) => boolean;

	// Project operation
	selector?: (item: any) => any;

	// Join operation
	otherStream?: Stream;
	thisKey?: (item: any) => any;
	otherKey?: (item: any) => any;

	// Union operation
	otherQuery?: Query<any>;

	// Optimization metadata for Algorithm 4.6
	isLinear?: boolean;
	isBilinear?: boolean;
}

/**
 * DBSP Fluent Query Builder
 *
 * Provides a chainable API for building complex queries over ZSet streams.
 * Automatically applies Algorithm 4.6 for optimal incremental execution.
 */

export class Query<T> {
	private operations: QueryOperation[] = [];

	// Start a query chain
	static from<T>(stream: Stream): Query<T> {
		return new Query<T>([
			{
				// ← Fixed: pass operations array to constructor
				type: 'source',
				stream: stream
			}
		]);
	}

	constructor(operations: QueryOperation[] = []) {
		this.operations = operations;
	}

	// Filter operation (σ in Table 1)
	where(predicate: (item: T) => boolean): Query<T> {
		return this.addOperation({
			type: 'filter',
			predicate: predicate,
			isLinear: true // For Algorithm 4.6 optimization
		});
	}

	// Projection operation (π in Table 1)
	select<U>(selector: (item: T) => U): Query<U> {
		return this.addOperation({
			type: 'project',
			selector: selector,
			isLinear: true
		}); // as Query<U>; // ← Add explicit cast
	}

	// Join operation (⊲⊳ in Table 1)
	join<U, K>(
		otherStream: Stream,
		thisKey: (item: T) => K,
		otherKey: (item: U) => K
	): Query<[T, U]> {
		return this.addOperation({
			type: 'join',
			otherStream: otherStream,
			thisKey: thisKey,
			otherKey: otherKey,
			isBilinear: true // For Theorem 3.4 optimization
		}); //as Query<[T, U]u;  // ← Add explicit cast
	}

	// Distinct operation (Table 1)
	distinct(): Query<T> {
		return this.addOperation({
			type: 'distinct',
			isLinear: false // Distinct is NOT linear
		});
	}

	// Union operation
	union(otherQuery: Query<T>): Query<T> {
		return this.addOperation({
			type: 'union',
			otherQuery: otherQuery,
			isLinear: true
		});
	}

	// Execute as regular (non-incremental) query
	execute(): Stream {
		// Get the source stream from first operation
		const sourceOp = this.operations.find((op) => op.type === 'source');
		if (!sourceOp || !sourceOp.stream) {
			throw new Error('Query must start with Query.from(stream)');
		}

		return this.buildCircuit().execute(sourceOp.stream);
	}

	// 🎯 FUTURE: Apply Algorithm 4.6 for automatic incremental execution
	// For now, just use regular execution
	autoIncremental(): Stream {
		const ivm = new IncrementalViewMaintenance();
		const optimizedCircuit = ivm.generateIncrementalPlan(this);

		// Get source stream for execution
		const sourceOp = this.operations.find((op) => op.type === 'source');
		if (!sourceOp || !sourceOp.stream) {
			throw new Error('Query must start with Query.from(stream)');
		}

		return optimizedCircuit.execute(sourceOp.stream);
	}

	private buildCircuit(): Circuit {
		const circuit = new Circuit();

		for (const op of this.operations) {
			switch (op.type) {
				case 'source':
					// Source is handled by the circuit input
					break;
				case 'filter':
					circuit.addOperator((stream) => stream.liftFilter(op.predicate!));
					break;
				case 'project':
					circuit.addOperator((stream) => stream.liftProject(op.selector!));
					break;
				case 'join':
					circuit.addBinaryOperator(
						(left, right) => left.liftJoin(right, op.thisKey!, op.otherKey!),
						op.otherStream!
					);
					break;
				case 'distinct':
					circuit.addOperator((stream) => stream.liftDistinct());
					break;
				case 'union':
					// Execute otherQuery to get its stream
					const otherQueryStream = op.otherQuery!.execute();
					circuit.addBinaryOperator((left, right) => left.liftUnion(right), otherQueryStream);
					break;
			}
		}

		return circuit;
	}

	private addOperation(op: QueryOperation): Query<any> {
		return new Query([...this.operations, op]);
	}

	// Helper for testing and debugging
	getOperations(): readonly QueryOperation[] {
		return this.operations;
	}
}
