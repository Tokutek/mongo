/**
*    Copyright (C) 2012 Tokutek Inc.
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

#ifndef TOKUDB_INVARIANT_H
#define TOKUDB_INVARIANT_H

#define invariant(x) { if (!(x)) { fprintf(stderr, "%s:%d Assertion `" #x "' failed\n", __FUNCTION__, __LINE__); fflush(stderr); massert(16399, "toku assert fail", 0); }}

#endif /* TOKUDB_INVARIANT_H */
