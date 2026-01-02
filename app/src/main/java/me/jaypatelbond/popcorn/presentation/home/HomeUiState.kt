package me.jaypatelbond.popcorn.presentation.home

import me.jaypatelbond.popcorn.domain.model.Movie

/**
 * UI state for the Home screen.
 */
data class HomeUiState(
    val trendingMovies: List<Movie> = emptyList(),
    val nowPlayingMovies: List<Movie> = emptyList(),
    val isTrendingLoading: Boolean = false,
    val isNowPlayingLoading: Boolean = false,
    val trendingError: String? = null,
    val nowPlayingError: String? = null,
    val isOffline: Boolean = false
) {
    val isLoading: Boolean
        get() = isTrendingLoading || isNowPlayingLoading
    
    val hasError: Boolean
        get() = trendingError != null || nowPlayingError != null
}
