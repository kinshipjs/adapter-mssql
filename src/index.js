// @ts-check
import { KinshipNonUniqueKeyError, KinshipSyntaxError, KinshipUnhandledDBError, KinshipUnknownDBError, KinshipValueCannotBeNullError } from '@kinshipjs/core/errors';
import mssql from 'mssql';

// polyfill for checking if number is Integer

Number.isInteger = Number.isInteger || function(value) {
    return typeof value === 'number' && 
        isFinite(value) && 
        Math.floor(value) === value;
};
  

/**
 * @param {number} value
 */
Number.isSafeInteger = Number.isSafeInteger || function (value) {
    return Number.isInteger(value) && Math.abs(value) <= Number.MAX_SAFE_INTEGER;
};

function getType(val) {
    switch(typeof val) {
        case "number": {
            if (Number.isSafeInteger(val)) {
                if (val > 2147483648 || val < -2147483648) {
                    return mssql.BigInt;
                }
                return mssql.Int;
            }
            return mssql.Float;
        }
        case "string": return mssql.VarChar;
        case "bigint": return mssql.BigInt;
        case "boolean": return mssql.Bit;
        case "object": {
            if(val instanceof Date) {
                return mssql.DateTime;
            }
        }
    }
    return mssql.VarChar;
}

/**
 * Executes a prepared statement as a command.
 * @param {mssql.ConnectionPool|mssql.Transaction} connectionOrTransaction
 * @param {string} cmd 
 * @param {any[]} argsArray
 * @returns {Promise<import('mssql').IProcedureResult<any>|undefined>}
 */
async function executeCommand(connectionOrTransaction, cmd, argsArray=[]) {
    /** @type {{[key: string]: { type: mssql.ISqlTypeFactory, value: any }}} */
    const args = Object.fromEntries(argsArray.map((a,n) => [`arg${n}`, { type: getType(a), value: a }]));
    cmd = argsArray.reduce((cmd, _, n) => cmd.replace("?", `@arg${n}`), cmd);
    /** @type {mssql.PreparedStatement=} */
    let ps;
    try {
        if(connectionOrTransaction instanceof mssql.Transaction) {
            ps = new mssql.PreparedStatement(connectionOrTransaction);
        } else {
            ps = new mssql.PreparedStatement(connectionOrTransaction);
        }
        for(const key in args) {
            if(args[key].value === null) {
                cmd = cmd.replace(`@${key}`, 'NULL');
            }
            ps = ps.input(key, { type: args[key].type });
        }
        await ps.prepare(cmd);
        const nameToValueMap = Object.fromEntries(Object.keys(args).map(k => [k, args[k].value]));
        const results = await ps.execute(nameToValueMap);
        return results;
    } finally {
        if(ps && ps.prepared) {
            await ps.unprepare();
        }
    }
}

/**
 * 
 * @param {string} tableOrColumn 
 * @param {boolean} ignoreSchema
 * @returns {string}
 */
function escape(tableOrColumn, ignoreSchema=false) {
    if(tableOrColumn.startsWith("MAX") || tableOrColumn.startsWith("MIN") || tableOrColumn.startsWith("SUM") || tableOrColumn.startsWith("COUNT") || tableOrColumn.startsWith("AVG")) {
        return tableOrColumn;
    }
    tableOrColumn = tableOrColumn.replaceAll("[", "").replaceAll("]", "");
    if(tableOrColumn.includes(".")) {
        const [schema, table] = tableOrColumn.split(".");
        if(ignoreSchema) return `[${table}]`;
        return `[${schema}].[${table}]`;
    }
    return `[${tableOrColumn}]`;
}

/**
 * 
 * @param {Date} date 
 */
function getDateString(date) {
    const year = date.getUTCFullYear().toString().padStart(4, '0');
    const month = (date.getUTCMonth()+1).toString().padStart(2, '0');
    const day = (date.getUTCDate()).toString().padStart(2, '0');
    const hours = date.getUTCHours().toString().padStart(2, '0');
    const mins = date.getUTCMinutes().toString().padStart(2, '0');
    return `${year}-${month}-${day} ${hours}:${mins}`;
}

/** @type {import('@kinshipjs/core/adapter').InitializeAdapterCallback<mssql.ConnectionPool>} */
export function adapter(connection) {
    return {
        syntax: {
            dateString: getDateString
        },
        aggregates: {
            total: "COUNT(*)",
            count: (table, col) => `COUNT(DISTINCT ${escape(table, true)}.${escape(col)})`,
            avg: (table, col) => `AVG(${escape(table, true)}.${escape(col)})`,
            max: (table, col) => `MAX(${escape(table, true)}.${escape(col)})`,
            min: (table, col) => `MIN(${escape(table, true)}.${escape(col)})`,
            sum: (table, col) => `SUM(${escape(table, true)}.${escape(col)})`
        },
        execute(scope) {
            return {
                async forQuery(cmd, args) {
                    try {
                        const results = await executeCommand(connection, cmd, args);
                        return /** @type {any} */ (results?.recordsets[0]);
                    } catch(err) {
                        throw handleError(err);
                    }
                },
                async forInsert(cmd, args) {
                    try {
                        const results = await executeCommand(scope.transaction ?? connection, `${cmd};SELECT SCOPE_IDENTITY() AS insertId;`, args);
                        const firstInsertId = (results?.recordsets[0][0].insertId - (results?.rowsAffected[0] ?? 0));
                        return Array.from(Array(results?.rowsAffected[0]).keys()).map((_, n) => (n+1) + firstInsertId);
                    } catch(err) {
                        throw handleError(err);
                    }
                },
                async forUpdate(cmd, args) {
                    try {
                        const results = await executeCommand(scope.transaction ?? connection, cmd, args);
                        return results ? results.rowsAffected[0] : 0
                    } catch(err) {
                        throw handleError(err);
                    }
                },
                async forDelete(cmd, args) {
                    try {
                        const results = await executeCommand(scope.transaction ?? connection, cmd, args);
                        return results ? results.rowsAffected[0] : 0
                    } catch(err) {
                        throw handleError(err);
                    }
                },
                async forTruncate(cmd, args) {
                    try {
                        const results = await executeCommand(scope.transaction ?? connection, cmd, args);
                        return results ? results.rowsAffected[0] : 0
                    } catch(err) {
                        throw handleError(err);
                    }
                },
                async forDescribe(cmd, args) {
                    const results = await executeCommand(connection, cmd);
                    /** @type {any} */
                    let set = {}
                    for(const field of results?.recordset ?? []) {
                        let defaultValue = getDefaultValueFn(field.Type, field.Default);
                        let type = field.Type.toLowerCase();
                        
                        loopThroughDataTypes:
                        for (const dataType in mssqlDataTypes) {
                            for(const dt of mssqlDataTypes[dataType]) {
                                if(type.startsWith(dt)) {
                                    type = dataType;
                                    break loopThroughDataTypes;
                                }
                            }
                        }
                        set[field.Field] = {
                            field: field.Field,
                            table: "",
                            alias: "",
                            isPrimary: field.Key !== null,
                            isIdentity: field.Identity,
                            isVirtual: field.Identity || field.Default?.includes("(getdate())"),
                            isNullable: field.Null === 'YES',
                            datatype: type,
                            defaultValue
                        };
                    }
                    return set;
                },
                async forTransaction() {
                    return {
                        begin: async () => {
                            const transaction = connection.transaction();
                            await transaction.begin();
                            return transaction;
                        },
                        commit: async (transaction) => {
                            await transaction.commit();
                        },
                        rollback: async (transaction) => {
                            await transaction.rollback();
                        }
                    };
                }
            }
        },
        serialize() {
            return {
                forQuery(data) {
                    const selects = getSelects(data.select);
                    const from = getFrom(data.from, data.limit, data.offset, data.order_by, data.where);
                    const where = getWhere(data.where);
                    const whereMain = getWhere(data.where, data.from[0].realName);
                    const groupBy = getGroupBy(data.group_by);
                    const orderBy = getOrderBy(data.order_by);
                    const limit = getLimit(data.limit);
                    const offset = getOffset(data.offset);
                    /** @type {any[]} */
                    let args = [];
                    let cmd = "";
                    if(data.from.length > 1 && whereMain.cmd === where.cmd) {
                        // if these are not equal, then the offset/limit was never applied in the `getFrom` function, so we have to apply them here.
                        if(whereMain.cmd !== where.cmd) {
                            cmd = `SELECT ${selects.cmd}\n\tFROM ${from.cmd}${where.cmd}${groupBy.cmd}${orderBy.cmd}${offset.cmd}${limit.cmd}`;
                            /** @type {any[]} */
                            args = args.concat(...[
                                selects.args,
                                from.args, 
                                where.args,
                                groupBy.args, 
                                orderBy.args,
                                offset.args,
                                limit.args,
                            ]);
                        } else {
                            cmd = `SELECT ${selects.cmd}\n\tFROM ${from.cmd}${where.cmd}${groupBy.cmd}${orderBy.cmd}`;
                            /** @type {any[]} */
                            args = args.concat(...[
                                selects.args,
                                from.args, 
                                where.args,
                                groupBy.args, 
                                orderBy.args
                            ]);
                        }
                    } else {
                        // this will throw an error if order by, limit, or offset are used not in conjunction of eachother.
                        isOrderByFetchOffset(data.order_by, data.limit, data.offset);
                        cmd = `SELECT ${selects.cmd}\n\tFROM ${from.cmd}${where.cmd}${groupBy.cmd}${orderBy.cmd}${offset.cmd}${limit.cmd}`;
                        args = args.concat(...[
                            selects.args, 
                            from.args, 
                            where.args,
                            groupBy.args, 
                            orderBy.args,
                            offset.args, 
                            limit.args, 
                        ]);
                    }

                    return { 
                        cmd, 
                        args
                    };
                },
                forInsert(data) {
                    let cmd = "";
                    let args = [];
                    const { table, columns, values } = data;
                    args = values.flat();
                    cmd += `INSERT INTO ${escape(table)} (${columns.map(c => escape(c)).join(', ')})\n\tVALUES\n\t\t${values.flatMap(v => `(${Array.from(Array(v.length).keys()).map(_ => '?')})`).join('\n\t\t,')}`;
                    return { cmd: cmd, args: args };
                },
                forUpdate(data) {
                    const { table, columns, where, explicit, implicit } = data;
                    const { cmd: explicitCmd, args: explicitArgs } = getExplicitUpdate({ table, columns, where, explicit });
                    const { cmd: implicitCmd, args: implicitArgs } = getImplicitUpdate({ table, columns, where, implicit });
                    return { 
                        cmd: explicitCmd !== '' ? explicitCmd : implicitCmd,
                        args: explicitCmd !== '' ? explicitArgs : implicitArgs
                    };
                },
                forDelete(data) {
                    const { table, where } = data;
                    const { cmd, args } = handleWhere(where);
                    return { cmd: `DELETE FROM ${escape(table)} ${cmd}`, args };
                },
                forTruncate(data) {
                    return { cmd: `TRUNCATE TABLE ${escape(data.table)}`, args: [] };
                },
                forDescribe(table) {
                    const [sch, tab] = table.replaceAll("[", "").replaceAll("]", "").split(".");
                    return { cmd: `SELECT SC.[COLUMN_NAME] AS [Field],
	SC.[IS_NULLABLE] AS [Null],
	SC.[DATA_TYPE] AS [Type],
    SC.[COLUMN_DEFAULT] AS [Default],
	C.[is_identity] AS [Identity],
	IC.[object_id] AS [Key]
  from (
		SELECT C.*
			FROM information_schema.columns C
			WHERE C.[TABLE_SCHEMA] = '${sch}'
				AND C.[TABLE_NAME] = '${tab}'
	) SC 
	LEFT JOIN sys.columns C 
		ON SC.[COLUMN_NAME] = C.[name]
    LEFT JOIN sys.key_constraints KC
		ON KC.[parent_object_id] = C.[object_id] AND kc.type = 'PK'
    LEFT JOIN sys.index_columns IC 
		ON KC.[parent_object_id] = IC.[object_id]  AND KC.unique_index_id = IC.index_id AND IC.column_id = C.column_id
 WHERE C.object_id = OBJECT_ID('${sch}.${tab}');`, args: [] };
                }
            }
        }
    }
}

/**
 * 
 * @param {Error} originalError 
 * @returns {Error}
 */
function handleError(originalError) {

    if(originalError instanceof mssql.RequestError) {
        const { number: code, message } = originalError;
        switch(code) {
            case 156: return new KinshipSyntaxError(message);
            case 515: return new KinshipValueCannotBeNullError(code, message);
            case 2627: return new KinshipNonUniqueKeyError(code, message);
            case undefined: {
                return new KinshipUnhandledDBError(`Unhandled error.`, -1, message);
            }
        }
    }
    return originalError;
}

// Use {stringToCheck}.startsWith({dataType}) where {dataType} is one of the data types in the array for the respective data type used in Kinship.
// e.g., let determinedDataType = mysqlDataTypes.string.filter(dt => s.startsWith(dt)).length > 0 ? "string" : ...
const mssqlDataTypes = {
    string: [
        "char", "varchar", "nvarchar",
        "binary", "varbinary",
        "tinyblob", "mediumblob", "longblob", "blob",
        "tinytext", "mediumtext", "longtext", "text",
        "enum",
        "set"
    ],
    int: [
        "tinyint", "smallint", "mediumint", "bigint", "int",
    ],
    float: [
        "float",
        "double",
        "decimal",
        "dec"
    ],
    boolean: [
        "bit",
        "bit(1)",
        "bool",
        "boolean"
    ],
    date: [
        "date",
        "time",
        "year"
    ]
};

/**
 * Serializes the recursive nested where arguments into one string.
 * @param {*} conditions 
 * @param {string} table 
 * @param {(n: number) => string} sanitize 
 * @returns {{ cmd: string, args: any[] }}
 */
function handleWhere(conditions, table="", sanitize=(n) => `?`) {
    if(!conditions) return { cmd: '', args: [] };
    let args = [];

    // function to filter out conditions that do not belong to table.
    // (this must be a map, as if it was just filter being used, then it would remove an entire subarray, when maybe that array has conditions)
    const mapFilter = (x) => {
        if(Array.isArray(x)) {
            const filtered = x.map(mapFilter).filter(x => x !== undefined);
            return filtered.length > 0 ? filtered : undefined;
        }
        if(table == "" || (x.length > 0 && x.table.endsWith(table + "__"))) {
            return x;
        }
        return undefined;
    }

    // function to reduce each condition to one appropriate clause string.
    const reduce = (prevStr, cond, depth=0) => {
        const tabs = Array.from(Array(depth + 2).keys()).map(_ => `\t`).join('');
        
        // nested conditions
        if(Array.isArray(cond)) {
            const [nextCond, ...remainder] = cond;

            // edge case: BETWEEN operator.
            if(nextCond.operator === "BETWEEN") {
                const column = `${escape(nextCond.table, true)}.${escape(nextCond.property)}`;
                const lowerBound = sanitize(args.length);
                const upperBound = sanitize(args.length+1);
                const reduceStart = `${nextCond.chain} (${column} ${nextCond.operator} ${lowerBound} AND ${upperBound}`;
                const reduced = remainder.reduce((a, b) => reduce(a, b, depth + 1), reduceStart);
                const cmd = `${prevStr} ${reduced})\n${tabs}`;
                args = args.concat(nextCond.value);
                return cmd;
            }
            let value;
            if (Array.isArray(nextCond.value)) {
                args = args.concat(nextCond.value);
                value = `(${nextCond.value.map((_,n) => sanitize(args.length+n)).join(',')})`;
            } else {
                args.push(nextCond.value);
                value = sanitize(args.length);
            }
            const column = `${escape(nextCond.table, true)}.${escape(nextCond.property)}`;
            const reduceStart = `${nextCond.chain} (${column} ${nextCond.operator} ${value}`;
            const reduced = remainder.reduce((a, b) => {
                return reduce(a, b, depth + 1);
            }, reduceStart);
            const cmd = `${prevStr} ${reduced})\n${tabs}`;
            return cmd;
        }
        
        // edge case: BETWEEN operator.
        if(cond.operator === "BETWEEN") {
            const column = `${escape(cond.table, true)}.${escape(cond.property)}`;
            const lowerBound = sanitize(args.length);
            const upperBound = sanitize(args.length+1);
            const cmd = prevStr + `${cond.chain} ${column} ${cond.operator} ${lowerBound} AND ${upperBound}\n${tabs}`;
            args = args.concat(cond.value);
            return cmd;
        }

        // single condition.
        let value;
        if (Array.isArray(cond.value)) {
            args = args.concat(cond.value);
            value = `(${cond.value.map((_, n) => sanitize(args.length + n)).join(',')})`;
        } else {
            args.push(cond.value);
            value = sanitize(args.length);
        }
        const column = `${escape(cond.table, true)}.${escape(cond.property)}`;
        const cmd = prevStr + `${cond.chain} ${column} ${cond.operator} ${value}\n${tabs}`;
        return cmd;
    };
    
    // map the array, filter out undefineds, then reduce the array to get the clause.
    /** @type {string} */
    const reduced = conditions.map(mapFilter).filter(x => x !== undefined).reduce(reduce, '');
    return {
        // if a filter took place, then the WHERE statement of the clause may not be there, so we replace.
        cmd: reduced.startsWith("WHERE") 
            ? reduced.trimEnd()
            : reduced.startsWith("AND") 
                ? reduced.replace("AND", "WHERE").trimEnd() 
                : reduced.replace("OR", "WHERE").trimEnd(),
        // arguments was built inside the reduce function.
        args
    };
}

/**
 * @param {string} type 
 * @param {any} defaultValue 
 * @returns {() => any}
 */
function getDefaultValueFn(type, defaultValue) {
    if(defaultValue !== null) {
        if(type.includes("tinyint")) {
            defaultValue = parseInt(defaultValue) === 1;
        } else if(type.includes("bigint")) {
            defaultValue = BigInt(defaultValue);
        } else if(type.includes("double")) {
            defaultValue = parseFloat(defaultValue);
        } else if(type.includes("date")) {
            defaultValue = Date.parse(defaultValue);
        } else if(type.includes("int")) {
            defaultValue = parseInt(defaultValue);
        } else {
            if(defaultValue === "(getdate())") {
                return () => new Date();
            }
        }
    }
    return () => defaultValue;
}

function isOrderByFetchOffset(orderBy, limit, offset) {
    if(orderBy && (limit != null) && (offset != null)) {
        return true;
    }
    if((limit != null) || (offset != null)) {
        throw new Error(`The Kinship MSSQL adapter requires [.take()], [.skip()], and [.sortBy()] to be used in conjunction with eachother.`);
    }
    return false;
}

function getSelects(select) {
    const cols = select.map(prop => {
        if(prop.alias === '') {
            return ``;
        }
        if(!("aggregate" in prop)) {
            return `${escape(prop.table, true)}.${escape(prop.column)} AS ${escape(prop.alias)}`;
        }
        // AVG(CAST(column_name AS FLOAT))
        if(!prop.column.startsWith("COUNT")) {
            prop.column = prop.column.replace(/^(.*)\((.*)\)$/, "$1(CAST($2 AS float))");
        }
        return `${escape(prop.column)} AS ${escape(prop.alias)}`;
    }).join('\n\t\t,');
    return {
        cmd: `${cols}`,
        args: []
    };
}

function getLimit(limit) {
    if(limit == undefined) return { cmd: "", args: [] };
    return {
        cmd: `\n\tFETCH NEXT ? ROWS ONLY`,
        args: [limit]
    };
}

function getOffset(offset) {
    if(offset == undefined) return { cmd: "", args: [] };
    return {
        cmd: `\n\tOFFSET ? ROWS`,
        args: [offset]
    };
}

function getFrom(from, limit, offset, orderBy, where) {
    let cmd = "";
    let args = [];
    if(from.length > 1) {
        const joiningTables = [];
        const [main, ...joins] = from;
        const whereCmd = getWhere(where);
        const whereCmdOnlyMain = getWhere(where, main.realName);
        const shouldSubQuery = isOrderByFetchOffset(orderBy, limit, offset) && (whereCmdOnlyMain.args.length > 0 || whereCmdOnlyMain.cmd == whereCmd.cmd);
        if (shouldSubQuery) {
            const orderByCmd = getOrderBy(orderBy);
            const limitCmd = getLimit(limit);
            const offsetCmd = getOffset(offset);
            const mainSubQuery = `(SELECT * FROM ${escape(main.realName)} ${orderByCmd.cmd} ${offsetCmd.cmd} ${limitCmd.cmd}) AS ${escape(main.alias, true)}`;
            args = args.concat(orderByCmd.args, offsetCmd.args, limitCmd.args);
            joiningTables.push(mainSubQuery);
        } else {
            joiningTables.push(`${escape(main.realName)} AS ${escape(main.alias, true)}`);
        }

        cmd = joiningTables.concat(joins.map(table => {
            const nameAndAlias = `${escape(table.realName)} AS ${escape(table.alias, true)}`;
            const onRefererKey = `${escape(table.refererTableKey.table, true)}.${escape(table.refererTableKey.column)}`;
            const onReferenceKey = `${escape(table.referenceTableKey.table, true)}.${escape(table.referenceTableKey.column)}`;
            return `${nameAndAlias}\n\t\t\tON ${onRefererKey} = ${onReferenceKey}`;
        })).join('\n\t\tLEFT JOIN ');
    } else {
        cmd = `${escape(from[0].realName)} AS ${escape(from[0].alias, true)}`
    }
    return { cmd, args };
}

function getGroupBy(group_by) {
    if(!group_by) return { cmd: "", args: [] };
    return {
        cmd: '\n\tGROUP BY ' + group_by.map(prop => `${escape(prop.alias)}`).join('\n\t\t,'),
        args: []
    };
}

function getOrderBy(order_by) {
    // @TODO when order by is called with columns from joined tables, this will fail. 
    // (may need to update core to add support for this)
    if(!order_by) return { cmd: "", args: [] };
    return {
        cmd: '\n\tORDER BY ' + order_by
            .map(prop => `${escape(prop.alias)} ${prop.direction}`).join('\n\t\t,'),
        args: []
    };
}

function getWhere(where, table="") {
    if(!where) return { cmd: "", args: [] };
    const whereInfo = handleWhere(where, table);
    return {
        cmd: `\n\t${whereInfo.cmd}`,
        args: whereInfo.args
    };
}

/**
 * @param {any} param0 
 * @returns 
 */
function getExplicitUpdate({ table, columns, where, explicit }) {
    if(!explicit) return { cmd: "", args: "" };
    const { values } = explicit;
    const { cmd: cmdWhere, args: cmdArgs } = getWhere(where);

    const setValues = `\n\t\t${values.map((v,n) => `${columns[n]} = ?`).join('\n\t\t,')}`;
    return {
        cmd: `UPDATE ${escape(table)}\n\tSET${setValues}${cmdWhere}`,
        args: values.concat(cmdArgs)
    }
}

/**
 * @param {any} param0 
 * @returns 
 */
function getImplicitUpdate({ table, columns, where, implicit }) {
    if(!implicit) { 
        return { cmd: "", args: [] };
    }
    const { primaryKeys, objects } = implicit;

    // initialize all of the cases.
    let cases = columns.reduce(
        (prev, initial) => ({ ...prev, [initial]: { cmd: 'CASE\n\t\t', args: [] }}), 
        {}
    );
    // set each column in a case when (Id = ?) statement.
    for (const record of objects) {
        for (const key in record) {
            for(const primaryKey of primaryKeys) {
                // ignore the primary key, we don't want to set that.
                if(key === primaryKey || !(key in cases)) continue;
                cases[key].cmd += `\tWHEN ${primaryKey} = ? THEN ?\n\t\t`;
                cases[key].args = [...cases[key].args, record[primaryKey], record[key]];
            }
        }
    }
    // finish each case command.
    Object.keys(cases).forEach(k => cases[k].cmd += `\tELSE ${escape(k)}\n\t\tEND`);

    // delete the cases that have no sets. (this covers the primary key that we skipped above.)
    for (const key in cases) {
        if (cases[key].args.length <= 0) {
            delete cases[key];
        }
    }
    const { cmd: cmdWhere, args: cmdArgs } = getWhere(where);
    return {
        cmd: `UPDATE ${escape(table)}\n\tSET\n\t\t${Object.keys(cases).map(k => `${escape(k)} = (${cases[k].cmd})`).join(',\n\t\t')}${cmdWhere}`,
        args: [...Object.keys(cases).flatMap(k => cases[k].args), ...cmdArgs]
    };
}

/**
 * Creates an MSSQL Connection Pool given a schema for the tables that are intended to be connected to.
 * @param {import('mssql').config} config
 * @returns {Promise<mssql.ConnectionPool>}
 */
export async function createMsSqlPool(config) {
    const cp = new mssql.ConnectionPool(config);
    await cp.connect();
    return cp;
}