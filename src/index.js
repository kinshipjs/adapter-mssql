// @ts-check
import mssql from 'mssql';

function getType(val) {
    switch(typeof val) {
        case "number": return mssql.Int;
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
 * @param {mssql.ConnectionPool|mssql.Transaction} connection
 * @param {string} cmd 
 * @param {any[]} argsArray
 * @returns {Promise<import('mssql').IProcedureResult<any>|undefined>}
 */
async function executeCommand(connection, cmd, argsArray=[]) {
    /** @type {{[key: string]: { type: mssql.ISqlTypeFactory, value: any }}} */
    const args = Object.fromEntries(argsArray.map((a,n) => [`arg${n}`, { type: getType(a), value: a }]));
    cmd = argsArray.reduce((cmd, _, n) => cmd.replace("?", `@arg${n}`), cmd);
    return new Promise(async (resolve, reject) => {
        /** @type {mssql.PreparedStatement=} */
        let ps = undefined;
        try {
            if(connection instanceof mssql.Transaction) {
                ps = new mssql.PreparedStatement(connection);
            } else {
                ps = new mssql.PreparedStatement(connection);
            }
            for(const key in args) {
                if(args[key].value === null) {
                    cmd = cmd.replace(`@${key}`, 'NULL');
                }
                ps = ps.input(key, { type: args[key].type });
            }
            console.log(cmd, argsArray);
            await ps.prepare(cmd);
            const nameToValueMap = Object.fromEntries(Object.keys(args).map(k => [k, args[k].value]));
            const results = await ps.execute(nameToValueMap);
            resolve(results);
        } catch(err) {
            reject(err);
        } finally {
            if(ps) {
                await ps.unprepare();
            }
        }
    });
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

/** @type {import('@kinshipjs/core/adapter').InitializeAdapterCallback<mssql.ConnectionPool>} */
export function adapter(connection) {
    /** @type {import('mssql').Transaction=} */
    let transaction;
    return {
        syntax: {
            dateString: (date) => `${date.getUTCFullYear()}-${date.getUTCMonth()}-${date.getUTCDate()} ${date.getUTCHours}:${date.getUTCMinutes()}`
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
                        if(transaction) {
                            const results = await executeCommand(transaction, cmd, args);
                            return /** @type {any} */ (results?.recordsets[0]);
                        } else {
                            const results = await executeCommand(connection, cmd, args);
                            return /** @type {any} */ (results?.recordsets[0]);
                        }
                    } catch(err) {
                        await transaction?.rollback();
                        throw handleError(err);
                    }
                },
                async forInsert(cmd, args) {
                    try {
                        if(transaction) {
                            const results = await executeCommand(connection, `${cmd};SELECT SCOPE_IDENTITY() AS insertId;`, args);
                            const firstInsertId = (results?.recordsets[0][0].insertId - (results?.rowsAffected[0] ?? 0));
                            return Array.from(Array(results?.rowsAffected[0]).keys()).map((_, n) => (n+1) + firstInsertId);
                        } else {
                            const results = await executeCommand(connection, `${cmd};SELECT SCOPE_IDENTITY() AS insertId;`, args);
                            const firstInsertId = (results?.recordsets[0][0].insertId - (results?.rowsAffected[0] ?? 0));
                            return Array.from(Array(results?.rowsAffected[0]).keys()).map((_, n) => (n+1) + firstInsertId);
                        }
                    } catch(err) {
                        await transaction?.rollback();
                        throw handleError(err);
                    }
                },
                async forUpdate(cmd, args) {
                    try {
                        if(transaction) {
                            const results = await executeCommand(transaction, cmd, args);
                            return results ? results.rowsAffected[0] : 0
                        } else {
                            const results = await executeCommand(connection, cmd, args);
                            return results ? results.rowsAffected[0] : 0
                        }
                    } catch(err) {
                        await transaction?.rollback();
                        throw handleError(err);
                    }
                },
                async forDelete(cmd, args) {
                    try {
                        if(transaction) {
                            const results = await executeCommand(transaction, cmd, args);
                            return results ? results.rowsAffected[0] : 0
                        } else {
                            const results = await executeCommand(connection, cmd, args);
                            return results ? results.rowsAffected[0] : 0
                        }
                    } catch(err) {
                        await transaction?.rollback();
                        throw handleError(err);
                    }
                },
                async forTruncate(cmd, args) {
                    try {
                        if(transaction) {
                            const results = await executeCommand(transaction, cmd, args);
                            return results ? results.rowsAffected[0] : 0
                        } else {
                            const results = await executeCommand(connection, cmd, args);
                            return results ? results.rowsAffected[0] : 0
                        }
                    } catch(err) {
                        await transaction?.rollback();
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
                            isVirtual: field.Identity,
                            isNullable: field.Null === 'YES',
                            datatype: type,
                            defaultValue
                        };
                    }
                    console.log(JSON.stringify(set, undefined, 2));
                    return set;
                },
                async forTransactionBegin() {
                    transaction = connection.transaction();
                    await transaction.begin();
                },
                async forTransactionEnd(cnn) {
                    await transaction?.commit();
                    transaction = undefined;
                }
            }
        },
        serialize() {
            return {
                forQuery(data) {
                    const selects = getSelects(data.select);
                    const from = getFrom(data.from, data.limit, data.offset);
                    const where = getWhere(data.where);
                    const groupBy = getGroupBy(data.group_by);
                    const orderBy = getOrderBy(data.order_by);
                    const limit = getLimit(data.limit);
                    const offset = getOffset(data.offset);
                    /** @type {any[]} */
                    let args = [];
                    let cmd = "";
                    if(data.from.length > 1) {
                        cmd = `SELECT ${selects.cmd}\n\tFROM ${from.cmd}${where.cmd}${groupBy.cmd}${orderBy.cmd}`;
                        /** @type {any[]} */
                        args = args.concat(...[
                            selects.args,
                            from.args, 
                            where.args,
                            groupBy.args, 
                            orderBy.args
                        ]);
                    } else {
                        cmd = `SELECT ${selects.cmd}\n\tFROM ${from.cmd}${where.cmd}${orderBy.cmd}${offset.cmd}${limit.cmd}${groupBy.cmd}`;
                        args = args.concat(...[
                            selects.args, 
                            from.args, 
                            where.args,
                            orderBy.args,
                            offset.args, 
                            limit.args, 
                            groupBy.args, 
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
                    return { cmd: `TRUNCATE ${escape(data.table)}`, args: [] };
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
    return originalError;
}

// Use {stringToCheck}.startsWith({dataType}) where {dataType} is one of the data types in the array for the respective data type used in Kinship.
// e.g., let determinedDataType = mysqlDataTypes.string.filter(dt => s.startsWith(dt)).length > 0 ? "string" : ...
const mssqlDataTypes = {
    string: [
        "char", "varchar", 
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
        if(x.table.includes(table)) {
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
            if(defaultValue === "getDate()") {
                return () => new Date();
            }
        }
    }
    return () => defaultValue;
}

function getSelects(select) {
    const cols = select.map(prop => {
        if(prop.alias === '') {
            return ``;
        }
        if(!("aggregate" in prop)) {
            return `${escape(prop.table, true)}.${escape(prop.column)} AS ${escape(prop.alias)}`;
        }
        return `${escape(prop.column)} AS ${escape(prop.alias)}`;
    }).join('\n\t\t,');
    return {
        cmd: `${cols}`,
        args: []
    };
}

function getLimit(limit) {
    if(!limit) return { cmd: "", args: [] };
    return {
        cmd: `\n\tFETCH NEXT ? ROWS ONLY`,
        args: [limit]
    };
}

function getOffset(offset) {
    if(!offset) return { cmd: "", args: [] };
    return {
        cmd: `\n\tOFFSET ? ROWS`,
        args: [offset]
    };
}

function getFrom(from, limit, offset) {
    let cmd = "";
    let args = [];
    if(from.length > 1) {
        const joiningTables = [];
        const [main, ...joins] = from;
        if(limit) {
            const limitCmd = getLimit(limit);
            const offsetCmd = getOffset(offset);
            const mainSubQuery = `(SELECT * FROM ${escape(main.realName)} ${limitCmd.cmd} ${offsetCmd.cmd}) AS ${escape(main.alias, true)}`;
            args = args.concat(limitCmd.args, offsetCmd.args);
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
    if(!order_by) return { cmd: "", args: [] };
    return {
        cmd: '\n\tORDER BY ' + order_by.map(prop => `${escape(prop.alias)}`).join('\n\t\t,'),
        args: []
    };
}

function getWhere(where) {
    if(!where) return { cmd: "", args: [] };
    const whereInfo = handleWhere(where);
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
                if(key === primaryKey) continue;
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