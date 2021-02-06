/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.consistency.checking.full;

import org.neo4j.consistency.report.PendingReferenceCheck;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.consistency.store.RecordReference;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static org.neo4j.consistency.store.RecordReference.SkippingReference.skipReference;

abstract class PropertyOwner<RECORD extends PrimitiveRecord> implements Owner
{
    abstract RecordReference<RECORD> record( RecordAccess records, PageCursorTracer cursorTracer );

    @Override
    public void checkOrphanage()
    {
        // default: do nothing
    }

    static class OwningNode extends PropertyOwner<NodeRecord>
    {
        private final long id;

        OwningNode( NodeRecord record )
        {
            this.id = record.getId();
        }

        @Override
        RecordReference<NodeRecord> record( RecordAccess records, PageCursorTracer cursorTracer )
        {
            return records.node( id, cursorTracer );
        }
    }

    static class OwningRelationship extends PropertyOwner<RelationshipRecord>
    {
        private final long id;

        OwningRelationship( RelationshipRecord record )
        {
            this.id = record.getId();
        }

        @Override
        RecordReference<RelationshipRecord> record( RecordAccess records, PageCursorTracer cursorTracer )
        {
            return records.relationship( id, cursorTracer );
        }
    }

    static class UnknownOwner extends PropertyOwner<PrimitiveRecord> implements RecordReference<PrimitiveRecord>
    {
        private PendingReferenceCheck<PrimitiveRecord> reporter;

        @Override
        RecordReference<PrimitiveRecord> record( RecordAccess records, PageCursorTracer cursorTracer )
        {
            // Getting the record for this owner means that some other owner replaced it
            // that means that it isn't an orphan, so we skip this orphan check
            // and return a record for conflict check that always is ok (by skipping the check)
            this.markInCustody();
            return skipReference();
        }

        @Override
        public void checkOrphanage()
        {
            PendingReferenceCheck<PrimitiveRecord> reporter;
            synchronized ( this )
            {
                reporter = this.reporter;
                this.reporter = null;
            }
            if ( reporter != null )
            {
                reporter.checkReference( null, null, null );
            }
        }

        synchronized void markInCustody()
        {
            if ( reporter != null )
            {
                reporter.skip();
                reporter = null;
            }
        }

        @Override
        public synchronized void dispatch( PendingReferenceCheck<PrimitiveRecord> reporter )
        {
            this.reporter = reporter;
        }
    }

    private PropertyOwner()
    {
        // only internal subclasses
    }
}
