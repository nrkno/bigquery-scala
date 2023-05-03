package no.nrk.bigquery

import no.nrk.bigquery.internal.{BQLiteralSyntax, BQShowSyntax, CatsShowInstances}

object syntax extends BQLiteralSyntax with BQShowSyntax with CatsShowInstances
