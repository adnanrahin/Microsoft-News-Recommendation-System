package org.microsoft.news.data_schemas

case class Behaviors(
                      impressionId: String,
                      userId: String,
                      time: String,
                      history: String,
                      impression: String,
                      clickCounter: Long,
                      nonClickCounter: Long
                    )
