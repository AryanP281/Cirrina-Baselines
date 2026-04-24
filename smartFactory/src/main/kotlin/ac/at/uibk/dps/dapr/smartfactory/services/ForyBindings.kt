package ac.at.uibk.dps.dapr.smartfactory.services

data class EmptyRequest(val dummy: String = "") {}

data class BeamDetectionResponse(val interrupted: Boolean)