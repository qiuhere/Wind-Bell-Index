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
package org.neo4j.kernel.impl.index.schema;

import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.neo4j.test.rule.OtherThreadRule;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NestedIndexReadersIT
{
    private static final int NODE_PER_ID = 3;
    private static final int IDS = 5;
    private static final Label LABEL = Label.label( "Label" );
    private static final String KEY = "key";

    @Rule
    public final ImpermanentDbmsRule db = new ImpermanentDbmsRule();
    @Rule
    public final OtherThreadRule<Void> t2 = new OtherThreadRule<>();

    @Test
    public void shouldReadCorrectResultsFromMultipleNestedReaders()
    {
        // given
        createIndex();
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < NODE_PER_ID; i++ )
            {
                createRoundOfNodes( tx );
            }
            tx.commit();
        }

        // when
        try ( Transaction tx = db.beginTx() )
        {
            // opening all the index readers
            List<ResourceIterator<Node>> iterators = new ArrayList<>();
            for ( int id = 0; id < IDS; id++ )
            {
                iterators.add( tx.findNodes( LABEL, KEY, id ) );
            }

            // then iterating over them interleaved should yield all the expected results each
            for ( int i = 0; i < NODE_PER_ID; i++ )
            {
                assertRoundOfNodes( iterators );
            }

            for ( ResourceIterator<Node> reader : iterators )
            {
                assertFalse( reader.hasNext() );
                reader.close();
            }

            tx.commit();
        }
    }

    @Test
    public void shouldReadCorrectResultsFromMultipleNestedReadersWhenConcurrentWriteHappens() throws Exception
    {
        // given
        createIndex();
        try ( Transaction tx = db.beginTx() )
        {
            for ( int id = 0; id < IDS; id++ )
            {
                for ( int i = 0; i < NODE_PER_ID; i++ )
                {
                    tx.createNode( LABEL ).setProperty( KEY, id );
                }
            }
            tx.commit();
        }

        // when
        try ( Transaction tx = db.beginTx() )
        {
            // opening all the index readers
            List<ResourceIterator<Node>> iterators = new ArrayList<>();
            for ( int id = 0; id < IDS; id++ )
            {
                iterators.add( tx.findNodes( LABEL, KEY, id ) );
            }

            // then iterating over them interleaved should yield all the expected results each
            for ( int i = 0; i < NODE_PER_ID; i++ )
            {
                assertRoundOfNodes( iterators );

                if ( i % 2 == 1 )
                {
                    // will be triggered on i == 1
                    t2.execute( nodeCreator() ).get();
                }
            }

            assertRoundOfNodes( iterators );

            for ( ResourceIterator<Node> reader : iterators )
            {
                assertFalse( reader.hasNext() );
                reader.close();
            }

            tx.commit();
        }
    }

    private void createRoundOfNodes( Transaction tx )
    {
        for ( int id = 0; id < IDS; id++ )
        {
            tx.createNode( LABEL ).setProperty( KEY, id );
        }
    }

    private void assertRoundOfNodes( List<ResourceIterator<Node>> iterators )
    {
        for ( int id = 0; id < IDS; id++ )
        {
            assertNode( iterators.get( id ), id );
        }
    }

    private WorkerCommand<Void,Void> nodeCreator()
    {
        return state ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                createRoundOfNodes( tx );
                tx.commit();
            }
            return null;
        };
    }

    private void assertNode( ResourceIterator<Node> reader, int id )
    {
        assertTrue( reader.hasNext() );
        Node node = reader.next();
        assertTrue( node.hasLabel( LABEL ) );
        assertEquals( "Expected node " + node + " (returned by index reader) to have 'id' property w/ value " + id,
                id, node.getProperty( KEY ) );
    }

    private void createIndex()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( LABEL ).on( KEY ).create();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 10, SECONDS );
            tx.commit();
        }
    }
}
