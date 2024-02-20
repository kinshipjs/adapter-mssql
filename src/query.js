//@ts-check
import { KinshipAdapterError } from "@kinshipjs/core/errors";
import { escapeColumn, escapeTable, handleWhere, joinArguments, joinCommands, mapFilter } from "./utility.js";

/**
 * @param {import("@kinshipjs/core/adapter").SerializationQueryHandlerData} data 
 */
export function forQuery(data) {
    const serializedData = getAllSerializedData(data);
    return handleQuery(serializedData);
}

/**
 * @param {import("@kinshipjs/core/adapter").SerializationQueryHandlerData} data 
 * @returns {SerializedData}
 */
function getAllSerializedData(data) {
    // handleFrom alters `data`, so it MUST be run before $orderBy, $limitOffsetOrderBy, and $limit.
    const $from = handleFrom(data);
    const $where = handleWhere(data.where);
    const $groupBy = handleGroupBy(data);
    const $select = handleSelect(data);
    const $orderBy = handleOrderBy(data);

    let $limitOffsetOrderBy = undefined;
    let $limit = undefined;
    if(!shouldSubQuery(data)) {
        $limitOffsetOrderBy = handleLimitOffsetOrderBy(data);
        $limit = handleLimit(data);
    }
    return {
        $from,
        $groupBy,
        $limit,
        $limitOffsetOrderBy,
        $orderBy,
        $where,
        $select
    };
}

/**
 * @param {SerializedData} serializedData
 * @param {number} nestLevel
 * The level of nesting that is occuring (e.g., if there is a sub query, then the nestLevel would become 2, so indentation is properly managed.)
 */
function handleQuery(serializedData, nestLevel=1) {
    const { 
        $from,
        $groupBy,
        $limit,
        $limitOffsetOrderBy,
        $orderBy,
        $select,
        $where,
    } = serializedData;

    let cmds = [];
    if($limitOffsetOrderBy) {
        cmds = [
            { args: $select?.args ?? [], cmd: `SELECT ${$select?.cmd}` },
            { args: $from?.args ?? [], cmd: `FROM ${$from?.cmd}` },
            $where,
            $groupBy,
            $limitOffsetOrderBy
        ];
    }
    else if($limit) {
        cmds = [
            { args: $limit?.args ?? [], cmd: `SELECT ${$limit?.cmd}` },
            $select,
            { args: $from?.args ?? [], cmd: `FROM ${$from?.cmd}` },
            $where,
            $groupBy,
            $orderBy
        ];
    }
    else {
        cmds = [
            { args: $select?.args ?? [], cmd: `SELECT ${$select?.cmd}` },
            { args: $from?.args ?? [], cmd: `FROM ${$from?.cmd}` },
            $where,
            $groupBy,
            $orderBy
        ];
    }
    
    return {
        cmd: joinCommands(cmds, nestLevel),
        args: joinArguments(cmds)
    };
}

/**
 * @param {import("@kinshipjs/core/adapter").SerializationQueryHandlerData} data 
 */
function handleFrom(data) {
    const { from, group_by, limit, offset, order_by, select, where } = data;
    const [main, ...includes] = from;
    
    let tables = [];
    let args = [];

    if(shouldSubQuery(data)) {
        const subQuery = getMainTableAsSubQuery(data);
        tables.push(subQuery.cmd);
        args = args.concat(subQuery.args);
    } else {
        tables.push(`${escapeTable(main.realName)} AS ${escapeTable(main.alias, true)}`);
    }

    tables = tables.concat(includes.map(table => {
        const nameAndAlias = `${escapeTable(table.realName)} AS ${escapeTable(table.alias, true)}`;
        const primaryKey = `${escapeTable(table.refererTableKey.table, true)}.${escapeColumn(table.refererTableKey.column)}`;
        const foreignKey = `${escapeTable(table.referenceTableKey.table, true)}.${escapeColumn(table.referenceTableKey.column)}`;
        return `${nameAndAlias}\n\t\t\tON ${primaryKey} = ${foreignKey}`;
    }));

    return {
        cmd: tables.join('\n\t\tLEFT JOIN '),
        args
    };
}

/**
 * @param {import("@kinshipjs/core/adapter").SerializationQueryHandlerData} data 
 */
function handleGroupBy(data) {
    const { from, group_by, limit, offset, order_by, select, where } = data;
    if(group_by) {
        return {
            cmd: 'GROUP BY ' + group_by.map(prop => `${escapeColumn(prop.alias)}`).join('\n\t\t,'),
            args: []
        };
    }
    return undefined;
}

/**
 * @param {import("@kinshipjs/core/adapter").SerializationQueryHandlerData} data 
 */
function handleLimit(data) {
    const { from, group_by, limit, offset, order_by, select, where } = data;
    const [main, ...includes] = from;

    if(limit) {
        return {
            cmd: `TOP (?)`,
            args: [limit]
        };
    }
    return undefined;
}

/**
 * @param {import("@kinshipjs/core/adapter").SerializationQueryHandlerData} data 
 * @param {string=} table
 */
function handleLimitOffsetOrderBy(data, table=undefined) {
    const { from, group_by, limit, offset, order_by, select, where } = data;
    const $orderBy = handleOrderBy(data, table);

    if(limit && offset && $orderBy) {
        const limitCmd = `FETCH NEXT ? ROWS ONLY`;
        const offsetCmd = `OFFSET ? ROWS`;
        return {
            cmd: `${$orderBy.cmd}\n\t${offsetCmd}\n\t${limitCmd}`,
            args: [...$orderBy.args, offset, limit]
        };
    }
    if(offset) {
        throw new KinshipAdapterError(`.skip() must be used in conjunction with .take() and .offset()`);
    }
    return undefined;
}

/**
 * @param {import("@kinshipjs/core/adapter").SerializationQueryHandlerData} data 
 * @param {string=} table
 */
function handleOrderBy(data, table=undefined) {
    const { from, group_by, limit, offset, order_by, select, where } = data;
    if(order_by) {
        // filter out props that are not part of this table.
        let orderBy = !table ? order_by : order_by.filter(prop => {
            return prop.table === table || prop.table.endsWith(table + "__");
        });
        if(orderBy.length <= 0) {
            return undefined;
        }
        const columns = orderBy.map(prop => `${escapeTable(prop.table, table === undefined)}.${escapeColumn(prop.column)} ${prop.direction}`).join('\n\t\t,');
        return {
            cmd: `ORDER BY ${columns}`,
            args: []
        }
    }
    return undefined;
}

/**
 * @param {import("@kinshipjs/core/adapter").SerializationQueryHandlerData} data 
 */
function handleSelect(data) {
    const { from, group_by, limit, offset, order_by, select, where } = data;
    const cols = select.map(prop => {
        if(prop.alias === '') {
            return ``;
        }
        if(!("aggregate" in prop)) {
            return `${escapeTable(prop.table, true)}.${escapeColumn(prop.column)} AS ${escapeColumn(prop.alias)}`;
        }
        if(!prop.column.startsWith("COUNT")) {
            prop.column = prop.column.replace(/^(.*)\((.*)\)$/, "$1(CAST($2 AS float))");
        }
        return `${escapeColumn(prop.column)} AS ${escapeColumn(prop.alias)}`;
    }).join('\n\t\t,');
    return {
        cmd: `${cols}`,
        args: []
    };
}

// ------------------------------------------- Utility -------------------------------------------

/**
 * 
 * @param {import("@kinshipjs/core/adapter").SerializationQueryHandlerData} data 
 */
function getMainTableAsSubQuery(data) {
    const { from, group_by, limit, offset, order_by, select, where } = data;
    const [main, ...includes] = from;

    const $limit = handleLimit(data);
    const $limitOffsetOrderBy = handleLimitOffsetOrderBy(data, main.realName);
    const $where = handleWhere(where, main.realName, false);
    const $groupBy = undefined;
    const $orderBy = handleOrderBy(data, main.realName);
    const $select = { cmd: "*", args: [] };
    const $from = { cmd: `${escapeTable(main.realName)}`, args: [] };

    const subQuery = handleQuery({ $from, $groupBy, $limit, $limitOffsetOrderBy, $orderBy, $select, $where }, 2);
    return {
        cmd: `(${subQuery.cmd}) AS ${escapeTable(main.alias, true)}`,
        args: subQuery.args
    };
}

/**
 * @param {import("@kinshipjs/core/adapter").SerializationQueryHandlerData} data 
 * @returns {boolean}
 */
function shouldSubQuery(data) {
    const { from, group_by, limit, offset, order_by, select, where } = data;
    const [main, ...includes] = from;
    const $where = handleWhere(data.where, main.realName);
    const $orderBy = handleOrderBy(data, main.realName);
    return !!((includes && includes.length > 0) && (limit || offset || $where || $orderBy)); 
}

// ------------------------------------------- Types -------------------------------------------

/**
 * @typedef {object} CommandInfo
 * @prop {string} cmd
 * @prop {any[]} args
 */

/**
 * @typedef {object} SerializedData
 * @prop {CommandInfo=} $from
 * @prop {CommandInfo=} $groupBy
 * @prop {CommandInfo=} $limit
 * @prop {CommandInfo=} $limitOffsetOrderBy
 * @prop {CommandInfo=} $orderBy
 * @prop {CommandInfo=} $select
 * @prop {CommandInfo=} $where
 */