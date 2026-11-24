// Run inside mongodb-0 through mongo-backup.py. Credentials come from the pod's Secret.
const env = process.env;
const restoring = Boolean(env.HARBORTRACE_RESTORE_TARGET);
const connection = new Mongo("mongodb://127.0.0.1:27017");
const name = restoring ? env.HARBORTRACE_RESTORE_TARGET : env.HARBORTRACE_DATABASE;
const app = connection.getDB(name);
if (restoring) {
    connection.getDB("admin").auth(env.MONGO_INITDB_ROOT_USERNAME, env.MONGO_INITDB_ROOT_PASSWORD);
} else {
    app.auth(env.HARBORTRACE_READER_USER, env.HARBORTRACE_READER_PASSWORD);
}

const {createHash} = require("crypto");
const collections = {};
for (const info of app.getCollectionInfos().sort((a, b) => a.name.localeCompare(b.name))) {
    if (info.type !== "collection" || info.name.startsWith("system.")) {
        throw new Error("Expected a HarborTrace application collection: " + info.name);
    }
    const collection = app.getCollection(info.name);
    const hash = createHash("sha256");
    let count = 0;
    collection.find().sort({_id: 1}).forEach(document => {
        hash.update(EJSON.stringify(document, {relaxed: false})).update("\n");
        count++;
    });
    const indexes = collection.getIndexes().sort((a, b) => a.name.localeCompare(b.name)).map(index => {
        // JSON objects compare unordered in Python; compound-index field order matters.
        const {ns, key, ...options} = index;
        return {...options, key: Object.entries(key)};
    });
    collections[info.name] = {count, sha256: hash.digest("hex"), indexes, options: info.options};
}
const active = app.serving_state.findOne({_id: "active"});
print(JSON.stringify({database: name, active_generation: active?.generation_id ?? null, collections}));
