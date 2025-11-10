[Usage]('./USAGE.md')

# TODO

- [ ] think more about how the view will be materialized. that should maybe be an external construct that can date a `delta` from `.processIncrement` and apply
- [ ] stateful-circuit and stateful-join are probably not what we want. let's rewrite to use `circuit.ts`. then optimize for perf. maybe have some external state we could pass to it to update/read from when processing incremental updates
- [ ] `optimzation.ts` should be removed. those methods should be defined elsewhere (stream or circuit probably)
- [ ] go through the paper again and highlight what we think are constructs and write out their responsibilities in a tldraw doc

watch https://www.youtube.com/watch?v=eYJA-ZBs-KM



watch https://www.youtube.com/watch?v=iT4k5DCnvPU

- DPSP streaming language has 4 operators
  - 2 operators for SQL
  - 2 for recursive queries
- An algorithm for converting an arbitrary DBSP program to an incremental DBSP program

if only supporting SQL:
  - relational: select, project, join, union, difference
  - computations on sets, bags, multisets
  - nested relations: group-by unnest
  - aggregation: min/max, sum, count, etc...

Streams
  - infinite vector
  - S<A> = streams with elements of type A
  - assume A has +, - operations and a 0

Stream Operator
  - takes a stream or multiple and outputs another stream
  - -> streams
  - [fn] operator -> a function that operates on scalars

diagram: i = input, o = output

i0,i1,i2 -> [fn] ->o0,o1,o2

Stream Operators:
  - Lifting: [↑fn] apply function fn to every item in a stream
    - can be chained together
  - Delay: [z^-1] the output is the input stream delayed by one step
    - first value is 0
    - with this we can build to fundamental blocks
      - diferentiation: 1,2,3,2 -> [D] -> 1,1,1,-1
        - this computes deltas
      - integration: 1,1,1,-1 -> [I] -> 1,2,3,2
        - this can reconstitute a stream from deltas

All databases are streaming databases
 - DB is a set of tables
 - a commited transaction is a **change** to a DB (and these are ordered)
 - all of those changes define T where T[t] is the t-th transaction
 - DB is a **stream** of snapshots
   - DB[t] is the contents of the db after t transactions have been executed

Now check this out!
T0,T1,T2,T3 -> [I] -> DB0,DB1,DB2,DB3,

Database snapshot is sum of all transactions so far


V is a view
[Q] is a query

so V[t] = Q(DB[t])
but if we want to define by streams we say
V = ↑Q(DB)

IVM algorithm:

T0,T1,T2 -> [I] -> [↑Q] -> [D] -> deltaV0, deltaV1, deltaV2

we'll call this [I] -> [↑Q] -> [D]
[Qdelta] for short hand

- [Qdelta] is a streaming system
- [Qdelta] needs to maintain internal state (even if Q is stateless)
- state is stores in the delay operators ([I] and [D])

There are two properties of operators:

- Linear operators: Q(a+b) = Q(a) + Q(b)
  - for a linear operator Q we have Q = Qdelta
  - why is this better?
    - you can skip the [I] and [D] operators. just do [Q]
    - these are equvalent
      - T0,T1,T2 -> [I] -> [↑Q] -> [D] -> deltaV0, deltaV1, deltaV2
      - T0,T1,T2 -> [↑Q] -> deltaV0, deltaV1, deltaV2 (this is much faster)
  - Most relational operators are linear!

Z-sets
  - each row has an integer weight
    - the weight can be positive, zero, or netagive
  - Can represent both TABLES and CHANGES to tables
    - Positive weight => row added
    - Negative weight => row removed
  - This can generalize to sets and multisets
    - a classic DB table is a Z-set where all weights are 1

Distinct operator takes a Zset and converts it into a set
  - it throws away rows with negative weights and converts all positive to 1

See Table 1 in paper for all of the circuit diagrams for SQL operations

- Bilinear operators (takes two inputs)
  - (Lifted) join is a bilinear operator



# Svelte library

Everything you need to build a Svelte library, powered by [`sv`](https://npmjs.com/package/sv).

Read more about creating a library [in the docs](https://svelte.dev/docs/kit/packaging).

## Creating a project

If you're seeing this, you've probably already done this step. Congrats!

```sh
# create a new project in the current directory
npx sv create

# create a new project in my-app
npx sv create my-app
```

## Developing

Once you've created a project and installed dependencies with `npm install` (or `pnpm install` or `yarn`), start a development server:

```sh
npm run dev

# or start the server and open the app in a new browser tab
npm run dev -- --open
```

Everything inside `src/lib` is part of your library, everything inside `src/routes` can be used as a showcase or preview app.

## Building

To build your library:

```sh
npm pack
```

To create a production version of your showcase app:

```sh
npm run build
```

You can preview the production build with `npm run preview`.

> To deploy your app, you may need to install an [adapter](https://svelte.dev/docs/kit/adapters) for your target environment.

## Publishing

Go into the `package.json` and give your package the desired name through the `"name"` option. Also consider adding a `"license"` field and point it to a `LICENSE` file which you can create from a template (one popular option is the [MIT license](https://opensource.org/license/mit/)).

To publish your library to [npm](https://www.npmjs.com):

```sh
npm publish
```
