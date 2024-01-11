//@ts-check
import { KinshipContext } from '@kinshipjs/core';
import { adapter, createMsSqlPool } from '../src/index.js';
import { testAdapter } from '@kinshipjs/adapter-tests';

const config = {
    server: "192.168.1.28",
    user: "sa",
    password: "mySuperSecretPassw0rd!",
    port: 15301,
    options: {
        encrypt: true,
        trustServerCertificate: true
    }
};

const pool = await createMsSqlPool({
    database: "chinook_ks_test",
    ...config
});

const chinookPool = await createMsSqlPool({
    database: "chinook",
    ...config
});

const connection = adapter(pool);

await testAdapter(connection, {
    albumsTableName: "dbo.Album",
    genresTableName: "dbo.Genre",
    playlistsTableName: "dbo.Playlist",
    playlistTracksTableName: "dbo.PlaylistTrack",
    tracksTableName: "dbo.Track",
    precision: 4
}, {
    printFail: true,
    printSuccess: true
});

process.exit(1);