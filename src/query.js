//@ts-check
import { KinshipAdapterError } from "@kinshipjs/core/errors";
import { escapeColumn, escapeTable, handleWhere, joinArguments, joinCommands } from "./utility.js";

/**
 * @param {import("@kinshipjs/core/adapter").SerializationQueryHandlerData} data 
 */
export function forQuery(data) {
    const $limit = handleLimit(data);
    const $orderBy = handleOrderBy(data);
    const $limitOffsetOrderBy = handleLimitOffsetOrderBy(data, { $orderBy });
    const $where = handleWhere(data.where);
    const $from = handleFrom(data, { $limit, $limitOffsetOrderBy });
    const $groupBy = handleGroupBy(data);
    const $select = handleSelect(data);

    return handleQuery({ $from, $groupBy, $limit, $limitOffsetOrderBy, $orderBy, $select, $where });
}

/**
 * @param {SerializedData} serializedData
 */
function handleQuery(serializedData) {
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
        cmd: joinCommands(cmds),
        args: joinArguments(cmds)
    };
}

/**
 * @param {import("@kinshipjs/core/adapter").SerializationQueryHandlerData} data 
 * @param {SerializedData} serializedData
 */
function handleFrom(data, serializedData) {
    const { from, group_by, limit, offset, order_by, select, where } = data;
    const { $limit, $limitOffsetOrderBy } = serializedData;
    const [main, ...includes] = from;
    
    let tables = [];
    let args = [];

    // if a limit, limit offset, order by, or where occurs when a join is occuring, then the main table should be sub-queried.
    if(includes && includes.length > 0 && ($limit || $limitOffsetOrderBy || order_by || (where && where.length > 0))) {
        const subQuery = getMainTableAsSubQuery(data, serializedData);
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
 * @param {SerializedData} serializedData
 */
function handleLimitOffsetOrderBy(data, serializedData) {
    const { from, group_by, limit, offset, order_by, select, where } = data;

    const { $orderBy } = serializedData;

    if(limit && offset && $orderBy) {
        const limitCmd = `FETCH NEXT ? ROWS ONLY`;
        const offsetCmd = `OFFSET ? ROWS`;
        return {
            cmd: `${$orderBy.cmd}\n\t${offsetCmd}\n\t${limitCmd}`,
            args: [...$orderBy.args, limit, offset]
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
        let orderBy = order_by;
        if(table) {
            orderBy = orderBy.filter(prop => prop.table === table);
        }
        const columns = orderBy.map(prop => `${escapeColumn(prop.alias)} ${prop.direction}`).join('\n\t\t,');
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
 * @param {SerializedData} serializedData 
 */
function getMainTableAsSubQuery(data, serializedData) {
    const { from, group_by, limit, offset, order_by, select, where } = data;
    const [main, ...includes] = from;

    const { $limit, $limitOffsetOrderBy, $orderBy } = serializedData;
    
    const $where = handleWhere(where, main.realName);
    const $groupBy = { cmd: "", args: [] };
    const $select = { cmd: "*", args: [] };
    const $from = { cmd: `${escapeTable(main.realName)} AS ${escapeTable(main.alias, true)}`, args: [] };

    const subQuery = handleQuery({ $from, $groupBy, $limit, $limitOffsetOrderBy, $orderBy, $select, $where });
    return {
        cmd: `(${subQuery.cmd})`,
        args: subQuery.args
    };
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