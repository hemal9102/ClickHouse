#include <Server/IcebergRESTCatalog/KeeperIcebergRESTCatalogStore.h>

#include <Common/Exception.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/escapeForFileName.h>
#include <base/unit.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogJSON.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ICEBERG_CATALOG_CONCURRENT_UPDATE;
    extern const int INCORRECT_DATA;
}

namespace
{

/// Data of the root node. Bump on any layout change so old data is refused, not misread.
constexpr auto FORMAT_MARKER = "IcebergRESTCatalog\nformat_version: 2";

constexpr size_t MAX_UPDATE_RETRIES = 10;

/// Keeper limits path depth and node data size. The handler maps `BAD_ARGUMENTS` to a 400 response.
constexpr size_t MAX_NAMESPACE_LEVELS = 16;
constexpr size_t MAX_NAMESPACE_LEVEL_LENGTH = 256;
constexpr size_t MAX_NAMESPACE_PROPERTIES_SIZE = 64_KiB;

void validateNamespace(const IcebergNamespaceName & name)
{
    if (name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Namespace must have at least one level");
    if (name.size() > MAX_NAMESPACE_LEVELS)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Namespace must have at most {} levels", MAX_NAMESPACE_LEVELS);
    for (const auto & level : name)
    {
        if (level.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Namespace levels must be non-empty");
        if (level.size() > MAX_NAMESPACE_LEVEL_LENGTH)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Namespace levels must be at most {} bytes", MAX_NAMESPACE_LEVEL_LENGTH);
        if (level.contains(NAMESPACE_LEVEL_SEPARATOR))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Namespace levels must not contain the unit separator 0x1F");
    }
}

void checkPropertiesSize(const std::map<String, String> & properties)
{
    size_t size = 0;
    for (const auto & [key, value] : properties)
        size += key.size() + value.size();
    if (size > MAX_NAMESPACE_PROPERTIES_SIZE)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'properties' must be at most {} bytes in total", MAX_NAMESPACE_PROPERTIES_SIZE);
}

String propertiesToJSON(const std::map<String, String> & properties)
{
    Poco::JSON::Object json;
    for (const auto & [key, value] : properties)
        json.set(key, value);
    return IcebergRESTCatalog::toJSONString(json);
}

std::map<String, String> propertiesFromJSON(const String & data, const String & path)
{
    const auto json = IcebergRESTCatalog::parseJSONObject(data, fmt::format("Namespace properties at {}", path));

    std::map<String, String> properties;
    for (const auto & [key, value] : *json)
    {
        if (!value.isString())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Namespace property '{}' at {} is not a string", key, path);
        properties[key] = value.extract<String>();
    }
    return properties;
}

/// Data of a table node.
struct TableNode
{
    String uuid;
    String location;
};

String tableNodeToJSON(const TableNode & node)
{
    Poco::JSON::Object json;
    json.set("uuid", node.uuid);
    json.set("location", node.location);
    return IcebergRESTCatalog::toJSONString(json);
}

TableNode tableNodeFromJSON(const String & data, const String & path)
{
    const auto json = IcebergRESTCatalog::parseJSONObject(data, fmt::format("Table node at {}", path));
    for (const auto * field : {"uuid", "location"})
        if (!json->has(field) || !json->get(field).isString())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Table node at {} has no string field '{}'", path, field);
    return TableNode{.uuid = json->getValue<String>("uuid"), .location = json->getValue<String>("location")};
}

/// Two table roots overlap when they are equal or one is a directory above the other. Both without a trailing slash.
bool locationsOverlap(const String & a, const String & b)
{
    if (a == b)
        return true;
    if (a.size() < b.size())
        return b.starts_with(a) && b[a.size()] == '/';
    return a.starts_with(b) && a[b.size()] == '/';
}

}

KeeperIcebergRESTCatalogStore::KeeperIcebergRESTCatalogStore(zkutil::GetZooKeeper get_zookeeper_, String root_path_)
    : root_path(std::move(root_path_))
    , get_zookeeper(std::move(get_zookeeper_))
{
}

zkutil::ZooKeeperPtr KeeperIcebergRESTCatalogStore::getZooKeeper() const
{
    /// Cheap on the hot path. Only a new session pays for `sync` and `initRoot`.
    std::lock_guard lock(mutex);
    auto zookeeper = get_zookeeper();
    if (zookeeper != initialized_zookeeper)
    {
        /// A new session may talk to another replica. Avoid reading stale state.
        zookeeper->sync(root_path);
        initRoot(zookeeper);
        /// Remember the session only after it passed the check, so a failed check is repeated.
        initialized_zookeeper = zookeeper;
    }
    return zookeeper;
}

void KeeperIcebergRESTCatalogStore::initRoot(const zkutil::ZooKeeperPtr & zookeeper) const
{
    zookeeper->createAncestors(root_path);

    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeCreateRequest(root_path, FORMAT_MARKER, zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(root_path + "/namespaces", "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(root_path + "/locations", "", zkutil::CreateMode::Persistent));
    Coordination::Responses responses;
    const auto code = zookeeper->tryMulti(ops, responses);
    if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNODEEXISTS)
        zkutil::KeeperMultiException::check(code, ops, responses);

    const auto marker = zookeeper->get(root_path);
    if (marker != FORMAT_MARKER)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Iceberg REST catalog data at {} has an unsupported format: expected {}, found {}",
            root_path,
            FORMAT_MARKER,
            marker);
}

String KeeperIcebergRESTCatalogStore::namespacePath(const IcebergNamespaceName & name) const
{
    /// Every method builds its path here, so this is the one place that checks the name.
    validateNamespace(name);
    String path = root_path;
    for (const auto & level : name)
        path += "/namespaces/" + escapeForFileName(level);
    return path;
}

String KeeperIcebergRESTCatalogStore::childNamespacesPath(const IcebergNamespaceName & name) const
{
    if (name.empty())
        return root_path + "/namespaces";
    return namespacePath(name) + "/namespaces";
}

String KeeperIcebergRESTCatalogStore::tablesPath(const IcebergNamespaceName & name) const
{
    return namespacePath(name) + "/tables";
}

String KeeperIcebergRESTCatalogStore::tablePath(const IcebergNamespaceName & name, const String & table) const
{
    return tablesPath(name) + "/" + escapeForFileName(table);
}

String KeeperIcebergRESTCatalogStore::tableUuidPath(const IcebergNamespaceName & name, const String & table, const String & uuid) const
{
    return tablePath(name, table) + "/" + uuid;
}

String KeeperIcebergRESTCatalogStore::locationsPath() const
{
    return root_path + "/locations";
}

String KeeperIcebergRESTCatalogStore::locationPath(const String & location) const
{
    return locationsPath() + "/" + escapeForFileName(location);
}

bool KeeperIcebergRESTCatalogStore::createNamespace(const IcebergNamespaceName & name, const std::map<String, String> & properties)
{
    auto component_guard = Coordination::setCurrentComponent("KeeperIcebergRESTCatalogStore::createNamespace");
    /// Validates the whole name before any parent is created.
    const auto path = namespacePath(name);
    checkPropertiesSize(properties);
    auto zookeeper = getZooKeeper();

    /// Each namespace is three nodes. Create them atomically so a crash cannot leave a partial namespace.
    const auto try_create = [&](const String & node_path, const std::map<String, String> & props)
    {
        Coordination::Requests ops;
        ops.emplace_back(zkutil::makeCreateRequest(node_path, propertiesToJSON(props), zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(node_path + "/namespaces", "", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(node_path + "/tables", "", zkutil::CreateMode::Persistent));
        Coordination::Responses responses;
        const auto code = zookeeper->tryMulti(ops, responses);
        if (code == Coordination::Error::ZNODEEXISTS)
            return false;

        zkutil::KeeperMultiException::check(code, ops, responses);
        return true;
    };

    /// Keep the tree walkable from the root.
    for (size_t level = 1; level < name.size(); ++level)
    {
        const auto parent_path = namespacePath(IcebergNamespaceName(name.begin(), name.begin() + level));
        if (!zookeeper->exists(parent_path))
            try_create(parent_path, {});
    }

    return try_create(path, properties);
}

bool KeeperIcebergRESTCatalogStore::namespaceExists(const IcebergNamespaceName & name) const
{
    auto component_guard = Coordination::setCurrentComponent("KeeperIcebergRESTCatalogStore::namespaceExists");
    return getZooKeeper()->exists(namespacePath(name));
}

std::optional<std::map<String, String>> KeeperIcebergRESTCatalogStore::getNamespaceProperties(const IcebergNamespaceName & name) const
{
    auto component_guard = Coordination::setCurrentComponent("KeeperIcebergRESTCatalogStore::getNamespaceProperties");
    const auto path = namespacePath(name);
    String data;
    if (!getZooKeeper()->tryGet(path, data))
        return std::nullopt;
    return propertiesFromJSON(data, path);
}

std::optional<UpdateNamespacePropertiesResult> KeeperIcebergRESTCatalogStore::updateNamespaceProperties(
    const IcebergNamespaceName & name, const std::map<String, String> & updates, const std::set<String> & removals)
{
    auto component_guard = Coordination::setCurrentComponent("KeeperIcebergRESTCatalogStore::updateNamespaceProperties");
    auto zookeeper = getZooKeeper();
    const auto path = namespacePath(name);

    /// The version check keeps concurrent updates from losing keys.
    for (size_t attempt = 0; attempt < MAX_UPDATE_RETRIES; ++attempt)
    {
        Coordination::Stat stat;
        String data;
        if (!zookeeper->tryGet(path, data, &stat))
            return std::nullopt;

        auto properties = propertiesFromJSON(data, path);
        UpdateNamespacePropertiesResult result;
        for (const auto & key : removals)
        {
            if (properties.erase(key) > 0)
                result.removed.push_back(key);
            else
                result.missing.push_back(key);
        }
        for (const auto & [key, value] : updates)
        {
            properties[key] = value;
            result.updated.push_back(key);
        }

        checkPropertiesSize(properties);

        const auto code = zookeeper->trySet(path, propertiesToJSON(properties), stat.version);
        if (code == Coordination::Error::ZOK)
            return result;
        if (code == Coordination::Error::ZNONODE)
            return std::nullopt;
        if (code != Coordination::Error::ZBADVERSION)
            throw zkutil::KeeperException::fromPath(code, path);
    }

    throw Exception(ErrorCodes::ICEBERG_CATALOG_CONCURRENT_UPDATE, "Cannot update properties of namespace at {}: too many concurrent updates", path);
}

std::vector<IcebergNamespaceName> KeeperIcebergRESTCatalogStore::listNamespaces(const IcebergNamespaceName & parent) const
{
    auto component_guard = Coordination::setCurrentComponent("KeeperIcebergRESTCatalogStore::listNamespaces");
    Strings children;
    const auto code = getZooKeeper()->tryGetChildren(childNamespacesPath(parent), children);
    if (code == Coordination::Error::ZNONODE)
        return {};
    if (code != Coordination::Error::ZOK)
        throw zkutil::KeeperException::fromPath(code, childNamespacesPath(parent));

    /// Keeper returns children in an unspecified order.
    std::sort(children.begin(), children.end());

    std::vector<IcebergNamespaceName> result;
    result.reserve(children.size());
    for (const auto & child : children)
    {
        auto name = parent;
        name.push_back(unescapeForFileName(child));
        result.push_back(std::move(name));
    }
    return result;
}

KeeperIcebergRESTCatalogStore::CreateTableOutcome KeeperIcebergRESTCatalogStore::createTable(
    const IcebergNamespaceName & ns, const String & table, const IcebergTablePointer & pointer, const String & location)
{
    auto component_guard = Coordination::setCurrentComponent("KeeperIcebergRESTCatalogStore::createTable");
    auto zookeeper = getZooKeeper();
    const auto table_path = tablePath(ns, table);
    const auto locations_path = locationsPath();

    /// The index is read, then written under a version check on `locations`. A concurrent create moves
    /// the version, so a check against a stale index fails and is repeated.
    /// TODO: The scan is linear in the number of tables of the warehouse.
    for (size_t attempt = 0; attempt < MAX_UPDATE_RETRIES; ++attempt)
    {
        Strings children;
        Coordination::Stat stat;
        const auto code = zookeeper->tryGetChildren(locations_path, children, &stat);
        if (code != Coordination::Error::ZOK)
            throw zkutil::KeeperException::fromPath(code, locations_path);

        for (const auto & child : children)
        {
            auto other = unescapeForFileName(child);
            if (locationsOverlap(location, other))
                return {.result = CreateTableResult::LocationOverlaps, .conflicting_location = std::move(other)};
        }

        /// Index of the table node op, used to tell which create failed.
        constexpr size_t TABLE_OP = 2;
        Coordination::Requests ops;
        ops.emplace_back(zkutil::makeSetRequest(locations_path, "", stat.version));
        ops.emplace_back(zkutil::makeCreateRequest(locationPath(location), pointer.uuid, zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(
            table_path, tableNodeToJSON({.uuid = pointer.uuid, .location = location}), zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(
            tableUuidPath(ns, table, pointer.uuid), pointer.metadata_location, zkutil::CreateMode::Persistent));
        Coordination::Responses responses;
        const auto multi_code = zookeeper->tryMulti(ops, responses);
        if (multi_code == Coordination::Error::ZOK)
            return {.result = CreateTableResult::Created, .conflicting_location = {}};
        if (multi_code == Coordination::Error::ZBADVERSION)
            continue;

        /// The version check passed, so the index is as scanned and only the table node can be in the way.
        if (multi_code == Coordination::Error::ZNODEEXISTS || multi_code == Coordination::Error::ZNONODE)
        {
            const auto failed_op = zkutil::getFailedOpIndex(multi_code, responses);
            if (multi_code == Coordination::Error::ZNODEEXISTS && failed_op == TABLE_OP)
                return {.result = CreateTableResult::TableExists, .conflicting_location = {}};
            /// The `tables` node is created with the namespace, so a missing parent means a missing namespace.
            if (multi_code == Coordination::Error::ZNONODE && failed_op == TABLE_OP)
                return {.result = CreateTableResult::NamespaceMissing, .conflicting_location = {}};
        }
        zkutil::KeeperMultiException::check(multi_code, ops, responses);
    }

    throw Exception(ErrorCodes::ICEBERG_CATALOG_CONCURRENT_UPDATE, "Cannot create table at {}: too many concurrent updates", table_path);
}

std::optional<IcebergTablePointer> KeeperIcebergRESTCatalogStore::getTable(const IcebergNamespaceName & ns, const String & table) const
{
    auto component_guard = Coordination::setCurrentComponent("KeeperIcebergRESTCatalogStore::getTable");
    auto zookeeper = getZooKeeper();
    const auto path = tablePath(ns, table);
    String data;
    if (!zookeeper->tryGet(path, data))
        return std::nullopt;
    IcebergTablePointer pointer{.uuid = tableNodeFromJSON(data, path).uuid, .metadata_location = {}};
    /// Missing here means the table was dropped between the two reads.
    Coordination::Stat stat;
    if (!zookeeper->tryGet(tableUuidPath(ns, table, pointer.uuid), pointer.metadata_location, &stat))
        return std::nullopt;
    pointer.version = ZooKeeperNodeVersion(stat.version);
    return pointer;
}

bool KeeperIcebergRESTCatalogStore::tableExists(const IcebergNamespaceName & ns, const String & table) const
{
    auto component_guard = Coordination::setCurrentComponent("KeeperIcebergRESTCatalogStore::tableExists");
    return getZooKeeper()->exists(tablePath(ns, table));
}

std::optional<Strings> KeeperIcebergRESTCatalogStore::listTables(const IcebergNamespaceName & ns) const
{
    auto component_guard = Coordination::setCurrentComponent("KeeperIcebergRESTCatalogStore::listTables");
    const auto path = tablesPath(ns);
    Strings children;
    const auto code = getZooKeeper()->tryGetChildren(path, children);
    if (code == Coordination::Error::ZNONODE)
        return std::nullopt;
    if (code != Coordination::Error::ZOK)
        throw zkutil::KeeperException::fromPath(code, path);

    Strings result;
    result.reserve(children.size());
    for (const auto & child : children)
        result.push_back(unescapeForFileName(child));
    /// Keeper returns children in an unspecified order.
    std::sort(result.begin(), result.end());
    return result;
}

bool KeeperIcebergRESTCatalogStore::dropTable(const IcebergNamespaceName & ns, const String & table)
{
    auto component_guard = Coordination::setCurrentComponent("KeeperIcebergRESTCatalogStore::dropTable");
    auto zookeeper = getZooKeeper();
    const auto path = tablePath(ns, table);
    String data;
    if (!zookeeper->tryGet(path, data))
        return false;
    const auto node = tableNodeFromJSON(data, path);

    /// Removing the uuid node first pins the table identity. A table re-created under the same name has another uuid.
    /// The first two ops are the table. A missing node there means the table is already gone.
    constexpr size_t LAST_TABLE_OP = 1;
    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeRemoveRequest(tableUuidPath(ns, table, node.uuid), -1));
    ops.emplace_back(zkutil::makeRemoveRequest(path, -1));
    /// A concurrent create may still see this entry and refuse. It never admits a location it should not.
    ops.emplace_back(zkutil::makeRemoveRequest(locationPath(node.location), -1));
    Coordination::Responses responses;
    const auto code = zookeeper->tryMulti(ops, responses);
    if (code == Coordination::Error::ZNONODE && zkutil::getFailedOpIndex(code, responses) <= LAST_TABLE_OP)
        return false;
    zkutil::KeeperMultiException::check(code, ops, responses);
    return true;
}

KeeperIcebergRESTCatalogStore::UpdateTableResult KeeperIcebergRESTCatalogStore::updateTable(
    const IcebergNamespaceName & ns, const String & table, const IcebergTablePointer & expected, const IcebergTablePointer & new_pointer)
{
    auto component_guard = Coordination::setCurrentComponent("KeeperIcebergRESTCatalogStore::updateTable");
    /// The uuid node pins the table identity. A table re-created under the same name has another uuid, so the set misses it.
    auto zookeeper = getZooKeeper();
    const auto path = tableUuidPath(ns, table, expected.uuid);
    const auto code = zookeeper->trySet(path, new_pointer.metadata_location, expected.version.toInt32());
    if (code == Coordination::Error::ZBADVERSION)
        return UpdateTableResult::VersionMismatch;
    if (code == Coordination::Error::ZNONODE)
        return zookeeper->exists(tablePath(ns, table)) ? UpdateTableResult::UuidMismatch : UpdateTableResult::TableMissing;
    if (code != Coordination::Error::ZOK)
        throw zkutil::KeeperException::fromPath(code, path);
    return UpdateTableResult::Updated;
}

}
