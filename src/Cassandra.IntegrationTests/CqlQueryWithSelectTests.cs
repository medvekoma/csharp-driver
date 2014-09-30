//
//      Copyright (C) 2014 DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

namespace Cassandra.IntegrationTests
{
    using System;
    using System.Configuration;
    using System.Linq;

    using Cassandra.Data.Linq;

    using NUnit.Framework;

    [TestFixture]
    public class CqlQueryWithSelectTests
    {
        private Cluster _cluster;

        private ISession _session;

        #region entities

        [Table]
        private class TestEntity
        {
            [PartitionKey]
            [Column("P1")]
            public string PartKey { get; set; }

            [ClusteringKey(1)]
            [Column("C1")]
            public int ClustKey1 { get; set; }

            [ClusteringKey(2)]
            [Column("C2")]
            public string ClustKey2 { get; set; }

            [Column("V1")]
            public string Value { get; set; }
        }

        private class TestResult
        {
            public string Pr1 { get; set; }

            public int Cr1 { get; set; }

            public string Cr2 { get; set; }

            public string Vr1 { get; set; }
        }

        #endregion

        [TestFixtureSetUp]
        public void SetupFixture()
        {
            var contactPoint = ConfigurationManager.AppSettings["ContactPoint"];
            var keyspaceName = ConfigurationManager.AppSettings["KeyspaceName"];
            _cluster = new Builder().AddContactPoint(contactPoint).Build();
            _session = _cluster.Connect();

            _session.CreateKeyspaceIfNotExists(keyspaceName);
            _session.ChangeKeyspace(keyspaceName);

            var table = _session.GetTable<TestEntity>();
            table.CreateIfNotExists();
        }

        [TestFixtureTearDown]
        public void TeardownFixture()
        {
            _session.Dispose();
            _cluster.Dispose();
        }

        [Test]
        public void SelectIntoTupleWorksFine()
        {
            var table = _session.GetTable<TestEntity>();
            table.CreateIfNotExists();
            var testEntity = new TestEntity() { PartKey = "Key1", ClustKey1 = 1, ClustKey2 = "CK1", Value = "Val1" };
            table.Insert(testEntity).Execute();

            var cqlQuery = table.Where(item => item.PartKey == testEntity.PartKey).Select(item => new Tuple<string, int>(item.Value, item.ClustKey1));

            var rows = cqlQuery.Execute().ToList();

            Console.WriteLine(cqlQuery);

            Assert.That(rows, Is.Not.Null);
            Assert.That(rows, Is.Not.Empty);
            var element = rows.First();

            Assert.That(element.Item1, Is.EqualTo(testEntity.Value));
            Assert.That(element.Item2, Is.EqualTo(testEntity.ClustKey1));

            // Assert.That(cqlQuery.ToString().StartsWith("SELECT \"x_ck1\", \"x_f1\" FROM \"x_t\""));
        }

        [Test]
        public void SelectIntoResultClassWorksFine()
        {
            var table = _session.GetTable<TestEntity>();
            table.CreateIfNotExists();
            var testEntity = new TestEntity() { PartKey = "Key1", ClustKey1 = 1, ClustKey2 = "CK1", Value = "Val1" };
            table.Insert(testEntity).Execute();

            var cqlQuery = table.Where(item => item.PartKey == testEntity.PartKey).Select(item => new TestResult() { Vr1 = item.Value, Cr1 = item.ClustKey1 });

            var rows = cqlQuery.Execute().ToList();

            Console.WriteLine(cqlQuery);

            Assert.That(rows, Is.Not.Null);
            Assert.That(rows, Is.Not.Empty);
            var element = rows.First();

            Assert.That(element.Vr1, Is.EqualTo(testEntity.Value));
            Assert.That(element.Cr1, Is.EqualTo(testEntity.ClustKey1));

            // Assert.That(cqlQuery.ToString().StartsWith("SELECT \"x_ck1\", \"x_f1\" FROM \"x_t\""));
        }
    }
}