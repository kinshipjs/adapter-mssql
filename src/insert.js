//@ts-check

import { escapeColumn, escapeTable, joinCommands } from "./utility.js";

/**
 * @param {import("@kinshipjs/core/adapter").SerializationInsertHandlerData} data
 */
export function forInsert(data) {
    let cmd = "";
    let args = [];
    const { table, columns, values } = data;
    args = /** @type {any[]} */ (values.flat().map(v => typeof v === 'number' && isNaN(v) ? null : v));

    const cols = columns.map(c => escapeColumn(c)).join('\n\t\t,');
    const vals = values.flatMap(v => `(${Array.from(Array(v.length).keys()).map(_ => '?')})`).join('\n\t\t,');
    return {
        cmd: joinCommands([
            { cmd: `INSERT INTO ${escapeTable(table)} (`, args: [] },
            { cmd: `\t${cols}`, args: [] },
            { cmd: `) VALUES `, args: [] },
            { cmd: `\t${vals}`, args: [] },
        ]),
        args
    }
}