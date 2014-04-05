/**
*    Copyright (C) 2014 Tokutek Inc.
*
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU Affero General Public License for more details.
*
*    You should have received a copy of the GNU Affero General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#pragma once

// All of these are potentially unused by the including file, so they all should be marked as unused.

// BSON fields for oplog entries
__attribute__((unused)) static const char KEY_STR_OP_NAME[] = "op";
__attribute__((unused)) static const char KEY_STR_NS[] = "ns";
__attribute__((unused)) static const char KEY_STR_ROW[] = "o";
__attribute__((unused)) static const char KEY_STR_OLD_ROW[] = "o";
__attribute__((unused)) static const char KEY_STR_NEW_ROW[] = "o2";
__attribute__((unused)) static const char KEY_STR_MODS[] = "m";
__attribute__((unused)) static const char KEY_STR_PK[] = "pk";
__attribute__((unused)) static const char KEY_STR_COMMENT[] = "o";
__attribute__((unused)) static const char KEY_STR_MIGRATE[] = "fromMigrate";

// values for types of operations in oplog
__attribute__((unused)) static const char OP_STR_INSERT[] = "i"; // normal insert
__attribute__((unused)) static const char OP_STR_CAPPED_INSERT[] = "ci"; // insert into capped collection
__attribute__((unused)) static const char OP_STR_UPDATE[] = "u"; // normal update with full pre-image and full post-image
__attribute__((unused)) static const char OP_STR_UPDATE_ROW_WITH_MOD[] = "ur"; // update with full pre-image and mods to generate post-image
__attribute__((unused)) static const char OP_STR_DELETE[] = "d"; // delete with full pre-image
__attribute__((unused)) static const char OP_STR_CAPPED_DELETE[] = "cd"; // delete from capped collection
__attribute__((unused)) static const char OP_STR_COMMENT[] = "n"; // a no-op
__attribute__((unused)) static const char OP_STR_COMMAND[] = "c"; // command

