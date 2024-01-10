// @ts-check
import mssql from 'mssql';
import { KinshipNonUniqueKeyError, KinshipSyntaxError, KinshipValueCannotBeNullError } from '@kinshipjs/core/errors';
import { forDelete } from './delete.js';
import { forInsert } from './insert.js';
import { forQuery } from './query.js';
import { forUpdate } from './update.js';
import { escapeColumn, escapeTable } from './utility.js';

/** @type {import('@kinshipjs/core/adapter').InitializeAdapterCallback<mssql.ConnectionPool>} */
export function adapter(connection) {
    return {
        syntax: {
            dateString: getDateString
        },
        aggregates: {
            total: "COUNT(*)",
            count: (table, col) => `COUNT(DISTINCT ${escapeTable(table, true)}.${escapeColumn(col)})`,
            avg: (table, col) => `AVG(${escapeTable(table, true)}.${escapeColumn(col)})`,
            max: (table, col) => `MAX(${escapeTable(table, true)}.${escapeColumn(col)})`,
            min: (table, col) => `MIN(${escapeTable(table, true)}.${escapeColumn(col)})`,
            sum: (table, col) => `SUM(${escapeTable(table, true)}.${escapeColumn(col)})`
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
                forQuery,
                forInsert,
                forUpdate,
                forDelete,
                forTruncate(data) {
                    return { cmd: `TRUNCATE TABLE ${escape(data.table)}`, args: [] };
                },
                forDescribe(table) {
                    const [schema, _table] = table.replaceAll("[", "").replaceAll("]", "").split(".");
                    return { cmd: `SELECT SC.[COLUMN_NAME] AS [Field],
	SC.[IS_NULLABLE] AS [Null],
	SC.[DATA_TYPE] AS [Type],
    SC.[COLUMN_DEFAULT] AS [Default],
	C.[is_identity] AS [Identity],
	IC.[object_id] AS [Key]
  from (
		SELECT C.*
			FROM information_schema.columns C
			WHERE C.[TABLE_SCHEMA] = '${schema}'
				AND C.[TABLE_NAME] = '${_table}'
	) SC 
	LEFT JOIN sys.columns C 
		ON SC.[COLUMN_NAME] = C.[name]
    LEFT JOIN sys.key_constraints KC
		ON KC.[parent_object_id] = C.[object_id] AND kc.type = 'PK'
    LEFT JOIN sys.index_columns IC 
		ON KC.[parent_object_id] = IC.[object_id]  AND KC.unique_index_id = IC.index_id AND IC.column_id = C.column_id
 WHERE C.object_id = OBJECT_ID('${schema}.${_table}');`, args: [] };
                }
            }
        },
        async asyncDispose() {
            await connection.close();
        },
        dispose() {
            connection.close();
        }
    }
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
                return originalError;
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

// polyfill for checking if number is Integer

/**
 * @param {number} value
 */
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