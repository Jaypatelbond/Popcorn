package me.jaypatelbond.popcorn.presentation.search

import me.jaypatelbond.popcorn.domain.model.Movie

/**
 * UI state for the Search screen.
 */
data class SearchUiState(
    val query: String = "",
    val results: List<Movie> = emptyList(),
    val isLoading: Boolean = false,
    val error: String? = null,
    val hasSearched: Boolean = false,
    val isOffline: Boolean = false
)
