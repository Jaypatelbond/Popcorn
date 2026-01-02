package me.jaypatelbond.popcorn.presentation.details

import me.jaypatelbond.popcorn.domain.model.Movie

/**
 * UI state for the Movie Details screen.
 */
data class MovieDetailsUiState(
    val movie: Movie? = null,
    val isLoading: Boolean = false,
    val error: String? = null,
    val isBookmarked: Boolean = false,
    val isOffline: Boolean = false
)
