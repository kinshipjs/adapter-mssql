//@ts-check

import { escapeColumn, escapeTable, handleWhere, joinArguments, joinCommands } from "./utility.js";

/**
 * @param {import("@kinshipjs/core/adapter").SerializationUpdateHandlerData} data
 */
export function forUpdate(data) {
    const { cmd: explicitCmd, args: explicitArgs } = getExplicitUpdate(data);
    const { cmd: implicitCmd, args: implicitArgs } = getImplicitUpdate(data);
    return { 
        cmd: explicitCmd !== '' ? explicitCmd : implicitCmd,
        args: explicitCmd !== '' ? explicitArgs : implicitArgs
    };
}

/**
 * @param {import("@kinshipjs/core/adapter").SerializationUpdateHandlerData} data
 */
function getExplicitUpdate({ table, columns, where, explicit }) {
    if(!explicit) return { cmd: "", args: [] };
    const { values } = explicit;
    const $where = handleWhere(where, undefined, false);
    const setValues = values.map((v,n) => `${columns[n]} = ?`).join('\n\t\t,');
    if(!$where) {
        return {
            cmd: joinCommands([
                `UPDATE ${escapeTable(table)}`,
                `SET ${setValues}`
            ]),
            args: values
        };
    }
    values.push(...$where.args);
    return {
        cmd: joinCommands([
            `UPDATE ${escapeTable(table)}`,
            `SET ${setValues}`,
            $where
        ]),
        args: (values)
    };
}

/**
 * @param {import("@kinshipjs/core/adapter").SerializationUpdateHandlerData} data
 */
function getImplicitUpdate({ table, columns, where, implicit }) {
    if(!implicit) { 
        return { cmd: "", args: [] };
    }
    const { primaryKeys, objects } = implicit;

    // initialize all of the cases.
    let cases = columns.reduce(
        (cases, column) => ({ ...cases, [column]: { cmd: 'CASE\n\t\t', args: [] }}), 
        {}
    );
    // set each column in a case when (Id = ?) statement.
    for (const record of objects) {
        for (const key in record) {
            for(const primaryKey of primaryKeys) {
                // ignore the primary key, we don't want to set that.
                if(key === primaryKey || !(key in cases)) continue;
                cases[key].cmd += `\tWHEN ${escapeTable(table)}.${escapeColumn(primaryKey)} = ? THEN ?\n\t\t`;
                cases[key].args = [...cases[key].args, record[primaryKey], record[key]];
            }
        }
    }
    // finish each case command.
    Object.keys(cases).forEach(k => cases[k].cmd += `\tELSE ${escapeTable(table)}.${escapeColumn(k)}\n\t\tEND`);

    // delete the cases that have no sets. (this covers the primary key that we skipped above.)
    for (const key in cases) {
        if (cases[key].args.length <= 0) {
            delete cases[key];
        }
    }

    const caseStrings = Object.keys(cases).map(k => `${escapeTable(table)}.${escapeColumn(k)} = (${cases[k].cmd})`).join(',\n\t\t');
    const $where = handleWhere(where, undefined, false);
    if(!$where) {
        return {
            cmd: joinCommands([
                `UPDATE ${escapeTable(table)}`,
                `SET`,
                `\t${caseStrings}`
            ]),
            args: joinArguments(Object.values(cases))
        };
    }

    return {
        cmd: joinCommands([
            `UPDATE ${escapeTable(table)}`,
            `SET`,
            caseStrings,
            $where
        ]),
        args: joinArguments([
            ...Object.values(cases),
            $where
        ])
    };
}