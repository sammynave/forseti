export class ZSet {
	private data = new Map<string, number>();
	private index = new Map<string, any>(); // Fast ID lookups

	findById(id: string): any {
		return this.index.get(id); // O(1)
	}

	// DBSP-style: add element with specified weight (default +1 for insertion)
	add(item: unknown, weight: number = 1) {
		const key = JSON.stringify(item);
		const currentWeight = this.data.get(key) || 0;
		const newWeight = currentWeight + weight;

		// DBSP semantics: keep all weights, including 0
		this.data.set(key, newWeight);
		// 🔧 FIX: Only index items with positive weights
		if (typeof item === 'object' && item && 'id' in item) {
			if (newWeight > 0) {
				this.index.set(item.id, item);
			} else {
				this.index.delete(item.id); // Remove from index if weight <= 0
			}
		}
	}

	plus(other: ZSet): ZSet {
		const result = new ZSet();

		// Get all unique keys from both Z-sets
		const allKeys = new Set([...this.data.keys(), ...other.data.keys()]);

		// For each key, add weights from both Z-sets
		for (const key of allKeys) {
			const thisWeight = this.data.get(key) ?? 0;
			const otherWeight = other.data.get(key) ?? 0;
			const combinedWeight = thisWeight + otherWeight;

			result.data.set(key, combinedWeight);

			const item = JSON.parse(key);
			if (typeof item === 'object' && item && 'id' in item && combinedWeight > 0) {
				result.index.set(item.id, item);
			}
		}

		return result;
	}

	zero(): ZSet {
		return new ZSet(); // Empty Z-set is the identity
	}

	negate(): ZSet {
		const result = new ZSet();
		for (const [key, weight] of this.data) {
			result.data.set(key, -weight);
		}
		return result;
	}

	// Convert to regular Todo array for display (only positive weights)
	get materialize() {
		return Array.from(this.data.entries())
			.filter(([_, weight]) => weight > 0)
			.map(([todoJson, _]) => JSON.parse(todoJson));
	}

	//__Filter/Selection (σ)__ - Linear Operator
	//From Table 1: σₚ(m)[x] = {m[x] if P(x), 0 otherwise}
	filter(predicate: (item: any) => boolean): ZSet {
		const result = new ZSet();

		for (const [key, weight] of this.data) {
			const item = JSON.parse(key);
			if (predicate(item)) {
				result.data.set(key, weight);
				// Update index if needed
				if (typeof item === 'object' && item && 'id' in item && weight > 0) {
					result.index.set(item.id, item);
				}
			}
		}

		return result;
	}

	// __Projection (π)__ - Linear Operator
	// From Table 1: π(i)[y] = Σₓ∈ᵢ,ₓ|c=y i[x]
	project<T>(selector: (item: any) => T): ZSet {
		const result = new ZSet();

		for (const [key, weight] of this.data) {
			const item = JSON.parse(key);
			const projected = selector(item);
			result.add(projected, weight);
		}

		return result;
	}

	// __Cartesian Product (×)__ - Bilinear Operator
	// From Table 1: `(a × b)((x,y)) = a[x] × b[y]`
	cartesianProduct(other: ZSet): ZSet {
		const result = new ZSet();

		for (const [keyA, weightA] of this.data) {
			for (const [keyB, weightB] of other.data) {
				const itemA = JSON.parse(keyA);
				const itemB = JSON.parse(keyB);
				const pair = [itemA, itemB];
				result.add(pair, weightA * weightB);
			}
		}

		return result;
	}

	// From Proposition 4.7: Converts Z-set to set by keeping only positive weights as 1
	distinct(): ZSet {
		const result = new ZSet();

		for (const [key, weight] of this.data) {
			if (weight > 0) {
				result.data.set(key, 1); // Set weight to 1
				const item = JSON.parse(key);
				if (typeof item === 'object' && item && 'id' in item) {
					result.index.set(item.id, item);
				}
			}
			// Items with weight <= 0 are omitted (removed from set)
		}

		return result;
	}

	// __Equi-Join (⊲⊳)__ - Bilinear Operator
	// From Table 1: (a ⊲⊳ b)((x,y)) = a[x] × b[y] if x|c₁ = y|c₂
	join<K>(other: ZSet, thisKey: (item: any) => K, otherKey: (item: any) => K): ZSet {
		const result = new ZSet();

		for (const [keyA, weightA] of this.data) {
			for (const [keyB, weightB] of other.data) {
				const itemA = JSON.parse(keyA);
				const itemB = JSON.parse(keyB);

				// Check if join condition is satisfied
				if (thisKey(itemA) === otherKey(itemB)) {
					const joinedItem = [itemA, itemB]; // Tuple of joined items
					result.add(joinedItem, weightA * weightB);
				}
			}
		}

		return result;
	}

	// __Set Difference (-)__
	// From Table 1: I1 EXCEPT I2 = distinct(I1 + (-I2))
	difference(other: ZSet): ZSet {
		return this.plus(other.negate()).distinct();
	}

	// __Set Intersection (∩)__
	// From Table 1: I1 INTERSECT I2 = special case of equi-join
	intersection(other: ZSet): ZSet {
		// Join on identity (same items)
		return this.join(
			other,
			(x) => JSON.stringify(x),
			(y) => JSON.stringify(y)
		).project(([x, y]) => x); // Project to just one copy
	}

	// Debug: show the raw Z-set data
	debug(): Map<string, number> {
		return new Map(this.data);
	}
}
