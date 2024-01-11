//@ts-check
import { WhereChain, WhereOperator } from '@kinshipjs/core';

/**
 * @param {(import('./query.js').CommandInfo|undefined)[]} data
 */
export function joinArguments(data) {
    const args = [];
    for(const ci of data) {
        if(!ci) continue;
        args.push(...ci.args);
    }
    return args;
}

/**
 * @param {(import('./query.js').CommandInfo|string|undefined)[]} data
 */
export function joinCommands(data, indentationCount=1) {
    const strings = [];
    for(const ci of data) {
        if(!ci) continue;
        if(typeof ci === 'string') {
            strings.push(ci);
        } else {
            strings.push(ci.cmd);
        }
    }
    const indentation = Array.from(Array(indentationCount).keys()).map(() => `\t`).join('');
    return strings.join(`\n${indentation}`);
}

/**
 * @param {string} column 
 */
export function escapeColumn(column) {
    if(column.startsWith("MAX") 
        || column.startsWith("MIN") 
        || column.startsWith("SUM") 
        || column.startsWith("COUNT") 
        || column.startsWith("AVG")
    ) {
        return column;
    }
    column = column.replace(/\[/g, "").replace(/\]/g, "");
    return `[${column}]`;
}

/**
 * @param {string} table 
 * @param {boolean} joinSchemaIntoTableName 
 */
export function escapeTable(table, joinSchemaIntoTableName=false) {
    table = table.replace(/\[/g, "").replace(/\]/g, "");
    if(table.includes(".")) {
        const [schema, _table] = table.split(".");
        if(joinSchemaIntoTableName) return `[${schema}_${_table}]`;
        return `[${schema}].[${_table}]`;
    }
    return `[${table}]`;
}

/**
 * 
 * @param {import("@kinshipjs/core/adapter").SerializationQueryHandlerData['where']} where
 * @param {string=} table 
 */
export function handleWhere(where, table=undefined, includeSchema=true) {
    if(!where || where.length <= 0) {
        return undefined;
    }

    // arguments must be passed as a reference here.
    let args = [];
    // reduce function to create the full WHERE condition.
    const reduce = createReduce(args, {
        spacing: 4,
        operators: {
            [WhereOperator.BETWEEN]: (x,y) => `BETWEEN ${x} AND ${y}`,
            [WhereOperator.IN]: (...values) => `IN (${values.join(',')})`
        },
        embedSchema: includeSchema
    });

    /** @type {string} */
    let cmd = where
        .map(prop => mapFilter(prop, table)) // map props that are not part of `table` to undefined.
        .filter(x => x !== undefined) // filter out undefined props
        .reduce((cmd, cond) => reduce(cmd, cond), ''); // reduce for the command.

    if(/^\s*AND|OR.*$/.test(cmd)) {
        cmd = cmd.replace(/^\s*AND|OR.*$/, "WHERE");
    }
    return {
        cmd,
        args
    };
}

/**
 * Various tools to set up the reduction function used for serializing WHERE clauses.
 * @typedef {object} WhereClauseReduceTools
 * @prop {{[key: WhereChain]: string}=} chains
 * A map of each `WhereChain` (used in Kinship) to the actual string it should use in your SQL engine.  
 * (e.g., AND, OR, AND NOT, OR NOT, etc.)
 * @prop {{[key: WhereOperator]: (...args: string[]) => string}=} operators
 * A map of each `WhereOperator` (used in Kinship) to a function that gets the actual string (with the values) 
 * it should use in your SQL engine.
 * 
 * __NOTE: The values are already sanitized, so you will not have to worry about sanitizing them again.__
 * 
 * e.g.,, in MSSQL:
 * ```js
 * const reduceTools = {
 *   operators: {
 *     [WhereOperator.BETWEEN]: (x,y) => `BETWEEN ${x} AND ${y}`
 *   }
 * }
 * ```
 * @prop {((n: number) => string)=} sanitize
 * Sanitizes the argument, where `n` is the index of the argument it would pass into `for.execute()`
 * 
 * e.g., in MySQL:
 * ```js
 * const reduceTools = {
 *   sanitize: (n) => `?`
 * }
 * ```
 * @prop {number=} spacing
 * Specifies the spacing for if the command should prettified before getting sent back to Kinship.
 * (the default for this is 4 [spaces], and it should remain this way so users can use `onSuccess` or `onFail` to monitor their commands.)
 * If `0` or `undefined` is passed in, then newlines are replaced with regular spaces.
 * @prop {boolean=} embedSchema
 * If true, then the table's schema will instead be embedded into the table name.  
 * e.g., `dbo.Table` becomes `[dbo_Table]`  
 * If false, then the table's schema will remain as a reference.  
 * e.g., `dbo.Table` becomes `[dbo].[Table]`  
 * default: true
 */

/**
 * @param {any[]} __argsReference
 * @param {WhereClauseReduceTools} tools
 */
function createReduce(
    __argsReference=[], 
    tools={}
) {
    const { 
        chains, 
        operators, 
        sanitize = (n) => '?', 
        spacing = 4,
        embedSchema = true
    } = tools;
    /**
     * @param {string} command
     * @param {any} condition
     */
    const reduce = (command, condition, depth=1) => {
        const padding = (spacing ?? 0) * 2;
        const numberOfIndents = (depth * (spacing ?? 0)) + padding;
        const indentation = Array.from(Array(numberOfIndents).keys()).map(_ => " ").join('');
        const prettify = `${spacing ? "\n" : " "}${indentation}`;

        if(Array.isArray(condition)) {
            const chain = condition[0].chain;
            condition[0].chain = "";
            const reduced = condition.reduce((command, condition) => reduce(command, condition, depth + 1), `${chain} (`);
            condition[0].chain = chain;
            return `${command}${reduced})${prettify}`;
        }
        let {
            chain,
            operator,
            property,
            table,
            value
        } = condition;
        
        if(Array.isArray(value)) {
            value = value.map((v,n) => {
                __argsReference.push(v);
                return sanitize(__argsReference.length + n);
            });
        } else {
            __argsReference.push(value);
        }

        chain = chains?.[chain] ?? chain;
        if(operators?.[operator]) {
            const values = /** @type {string[]} */ (Array.isArray(value) ? value : [value]);
            return `${command}${chain} ${escapeTable(table, embedSchema)}.${escapeColumn(property)} ${operators[operator](...values)} ${prettify}`;
        }
        return `${command}${chain} ${escapeTable(table, embedSchema)}.${escapeColumn(property)} ${operator} ${sanitize(__argsReference.length)}${prettify}`;
    }

    return reduce;
}

/**
 * Maps all clause properties that are not part of a table to undefined, otherwise they stay.  
 * This is intended to be used in conjunction with `.filter(o => o !== undefined)`
 * @param {import('@kinshipjs/core/adapter').WhereClausePropertyArray[number]} prop 
 * @param {string=} table
 */
export function mapFilter(prop, table=undefined) {
    if(!table) {
        return prop;
    }
    if(Array.isArray(prop)) {
        const filtered = prop.map((prop) => mapFilter(prop, table)).filter(x => x !== undefined);
        return filtered.length > 0 ? filtered : undefined;
    }
    if(prop.table === table || prop.table.endsWith(table + "__")) {
        return prop;
    }
    return undefined;
}