#ifndef UTIL_VERSION_HEADER
#define UTIL_VERSION_HEADER

#include <string>

#include "mongo/base/string_data.h"

namespace mongo {
    struct BSONArray;

    // mongo version
    extern const char mongodbVersionString[];
    extern const char tokumxVersionString[];
    extern const BSONArray versionArray;
    std::string fullVersionString();
    std::string mongodVersion();
    int versionCmp(StringData rhs, StringData lhs); // like strcmp

    const char * gitVersion();
    void printGitVersion();

    std::string sysInfo();
    void printSysInfo();

    const char *tokukvVersion();
    void printTokukvVersion();

    void show_warnings();

}  // namespace mongo

#endif  // UTIL_VERSION_HEADER
