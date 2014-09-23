package com.socrata.pg.store.events

import com.socrata.datacoordinator.secondary.{Delete, Insert, Operation, Update}
import com.socrata.datacoordinator.truth.metadata.{CopyInfo => TruthCopyInfo, DatasetCopyContext}
import com.socrata.pg.error.RowSizeBufferSqlErrorResync
import com.socrata.pg.store.PGSecondaryUniverse
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.typesafe.scalalogging.slf4j.Logging

case class RowDataUpdatedHandler(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], truthCopyInfo: TruthCopyInfo, ops: Seq[Operation[SoQLValue]]) extends Logging {
  val truthSchema = pgu.datasetMapReader.schema(truthCopyInfo)

  val copyCtx = new DatasetCopyContext[SoQLType](truthCopyInfo, truthSchema)
  val loader = pgu.prevettedLoader(copyCtx, pgu.logger(truthCopyInfo.datasetInfo, "test-user"))

  RowSizeBufferSqlErrorResync.guard(loader.conn) {
    ops.foreach {
      o =>
        logger.debug("Got row operation: {}", o)
        o match {
          case Insert(sid, row) => loader.insert(sid, row)
          case Update(sid, row) => loader.update(sid, None, row)
          case Delete(sid) => loader.delete(sid, None)
        }
    }
    loader.flush()
  }
}