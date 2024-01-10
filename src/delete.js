//@ts-check

import { escapeTable, handleWhere, joinCommands } from "./utility.js";

/**
 * 
 * @param {import("@kinshipjs/core/adapter").SerializationDeleteHandlerData} data 
 * @returns 
 */
export function forDelete(data) {
    const { table, where } = data;
    const $where = handleWhere(where, undefined, false);
    if(!$where) {
        return { 
            cmd: `DELETE FROM ${escapeTable(table)}`, 
            args: [] 
        };
    }
    return { 
        cmd: joinCommands([
            `DELETE FROM ${escapeTable(table)}`,
            $where
        ]), 
        args: $where.args 
    };
}