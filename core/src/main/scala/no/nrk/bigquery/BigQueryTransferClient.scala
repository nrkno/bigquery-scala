package no.nrk.bigquery

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.syntax.traverse._
import com.google.api.gax.core.FixedCredentialsProvider
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.bigquery.DatasetId
import com.google.cloud.bigquery.datatransfer.v1._
import com.google.protobuf.{Struct, Value}
import no.nrk.bigquery.BigQueryTransferClient.{TransferConfigFailed, TransferFailed, TransferStatus, TransferSucceeded}
import org.typelevel.log4cats.slf4j.{Slf4jFactory, loggerFactoryforSync}

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.CollectionConverters._

class BigQueryTransferClient(transferClient: DataTransferServiceClient) {
  protected lazy val logger = Slf4jFactory.getLogger[IO]

  def getTransferConfig(
      transferConfigName: TransferConfigName
  ): IO[Option[TransferConfig]] =
    IO.interruptible(
      transferClient.listTransferConfigs(
        s"projects/${transferConfigName.getProject}/locations/${transferConfigName.getLocation}"
      )
    ).map(
      _.iterateAll().asScala.find(config => config.getDisplayName == transferConfigName.getTransferConfig)
    )

  def buildTransferConfig(
      sourceDatasetId: DatasetId,
      destinationDatasetId: DatasetId,
      transferConfigName: TransferConfigName
  ): TransferConfig =
    TransferConfig.newBuilder
      .setDisplayName(transferConfigName.getTransferConfig)
      .setDestinationDatasetId(destinationDatasetId.getDataset)
      .setDataSourceId("cross_region_copy")
      .setDatasetRegion(transferConfigName.getLocation)
      .setScheduleOptions(
        ScheduleOptions.newBuilder().setDisableAutoScheduling(true)
      )
      .setParams(
        Struct.newBuilder
          .putFields(
            "source_project_id",
            Value.newBuilder.setStringValue(sourceDatasetId.getProject).build
          )
          .putFields(
            "source_dataset_id",
            Value.newBuilder.setStringValue(sourceDatasetId.getDataset).build
          )
          .build
      )
      .build

  def createTransferConfig(
      projectId: String,
      transferConfig: TransferConfig
  ): IO[TransferConfig] = {
    val projectName = ProjectName.of(projectId)
    val transferRequest = CreateTransferConfigRequest.newBuilder
      .setParent(projectName.toString)
      .setTransferConfig(transferConfig)
      .build
    IO.interruptible(transferClient.createTransferConfig(transferRequest))
  }

  def startDatasetTransfer(transferConfig: TransferConfig): IO[TransferStatus] =
    for {
      _ <- logger.info(
        s"Starting a run in transferconfig ${transferConfig.getDisplayName}"
      )
      startRequest = StartManualTransferRunsRequest.newBuilder
        .setParent(transferConfig.getName)
        .setRequestedRunTime(transferConfig.getUpdateTime)
        .build()
      runResponse <- IO.interruptible(
        transferClient.startManualTransferRuns(startRequest)
      )
      activeRun = runResponse.getRunsList.asScala.headOption
      transferStatus <- activeRun.traverse(activeRun =>
        pollDatasetTransfer(activeRun, 15.seconds)(retry =
          IO.interruptible(transferClient.getTransferRun(activeRun.getName))).attempt.map {
          case Left(err) => TransferFailed(activeRun, err)
          case Right(transfer) => transfer
        })
    } yield transferStatus match {
      case Some(transfer) => transfer
      case None => TransferConfigFailed(transferConfig)
    }

  private def pollDatasetTransfer(
      runningTransfer: TransferRun,
      baseDelay: FiniteDuration
  )(
      retry: IO[TransferRun]
  ): IO[TransferStatus] = {
    def waitAndPoll: IO[TransferStatus] =
      IO.sleep(baseDelay) >> retry.flatMap(running => go(running))

    def go(runningTransfer: TransferRun): IO[TransferStatus] =
      runningTransfer.getState match {
        case TransferState.PENDING =>
          logger.info(s"${runningTransfer.getName} is pending.") >> waitAndPoll
        case TransferState.RUNNING =>
          logger.info(s"${runningTransfer.getName} is running.") >> waitAndPoll
        case TransferState.SUCCEEDED =>
          logger.info(s"${runningTransfer.getName} succeeded.") >> IO.pure(
            TransferSucceeded(runningTransfer)
          )

        case TransferState.FAILED =>
          logger.error(s"${runningTransfer.getName} failed.") >> IO.pure(
            TransferFailed(
              runningTransfer,
              new Exception(
                s"${runningTransfer.getName} failed for unknown reason."
              )
            )
          )
        case TransferState.CANCELLED =>
          logger.error(s"${runningTransfer.getName} cancelled.") >> IO.pure(
            TransferFailed(
              runningTransfer,
              new Exception(
                s"${runningTransfer.getName} cancelled for unknown reason."
              )
            )
          )

        case TransferState.TRANSFER_STATE_UNSPECIFIED =>
          logger.info(
            s"${runningTransfer.getName} in unspecified state."
          ) >> waitAndPoll
        case TransferState.UNRECOGNIZED =>
          logger.info(
            s"${runningTransfer.getName} in unrecognized state."
          ) >> waitAndPoll

      }

    IO.sleep(baseDelay) >> go(runningTransfer)
  }
}

object BigQueryTransferClient {
  sealed trait TransferStatus
  case class TransferSucceeded(transferRun: TransferRun) extends TransferStatus
  case class TransferFailed(transferRun: TransferRun, error: Throwable) extends TransferStatus
  case class TransferConfigFailed(transferConfig: TransferConfig) extends TransferStatus

  def resource(
      credentials: ServiceAccountCredentials
  ): Resource[IO, BigQueryTransferClient] = {
    val dataTransferServiceClient = IO.blocking(
      DataTransferServiceClient.create(
        DataTransferServiceSettings
          .newBuilder()
          .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
          .build()
      )
    )
    Resource
      .fromAutoCloseable(dataTransferServiceClient)
      .map(new BigQueryTransferClient(_))
  }

}
