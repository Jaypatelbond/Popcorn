package me.jaypatelbond.popcorn.presentation.search

import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import dagger.hilt.android.lifecycle.HiltViewModel
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.debounce
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import me.jaypatelbond.popcorn.domain.repository.MovieRepository
import me.jaypatelbond.popcorn.util.Resource
import javax.inject.Inject

/**
 * ViewModel for the Search screen with debounced search.
 */
@OptIn(FlowPreview::class, kotlinx.coroutines.ExperimentalCoroutinesApi::class)
@HiltViewModel
class SearchViewModel @Inject constructor(
    private val movieRepository: MovieRepository,
    private val networkMonitor: me.jaypatelbond.popcorn.util.NetworkMonitor
) : ViewModel() {

    private val _uiState = MutableStateFlow(SearchUiState())
    val uiState: StateFlow<SearchUiState> = _uiState.asStateFlow()

    private val searchQuery = MutableStateFlow("")
    private val retryTrigger = MutableStateFlow(0)

    init {
        observeNetworkStatus()
        
        // Debounced search - waits 500ms after user stops typing
        viewModelScope.launch {
            kotlinx.coroutines.flow.combine(
                searchQuery
                    .debounce(500)
                    .distinctUntilChanged()
                    .filter { it.length >= 2 },
                retryTrigger
            ) { query, _ -> query }
            .flatMapLatest { query ->
                movieRepository.searchMovies(query)
            }
            .collect { result ->
                when (result) {
                    is Resource.Loading -> {
                        _uiState.update {
                            it.copy(
                                isLoading = true,
                                error = null,
                                hasSearched = true
                            )
                        }
                    }
                    is Resource.Success -> {
                        _uiState.update {
                            it.copy(
                                isLoading = false,
                                results = result.data ?: emptyList(),
                                error = null
                            )
                        }
                    }
                    is Resource.Error -> {
                        _uiState.update {
                            it.copy(
                                isLoading = false,
                                error = result.message,
                                results = result.data ?: it.results
                            )
                        }
                    }
                }
            }
        }
    }

    private fun observeNetworkStatus() {
        viewModelScope.launch {
            networkMonitor.isOnline.collect { isOnline ->
                _uiState.update { it.copy(isOffline = !isOnline) }
            }
        }
    }

    fun retry() {
        retryTrigger.update { it + 1 }
    }

    /**
     * Update search query - triggers debounced search.
     */
    fun onQueryChange(query: String) {
        _uiState.update { it.copy(query = query) }
        searchQuery.value = query

        // Clear results if query is empty
        if (query.isEmpty()) {
            _uiState.update { 
                it.copy(
                    results = emptyList(),
                    hasSearched = false,
                    error = null
                )
            }
        }
    }

    /**
     * Clear search.
     */
    fun clearSearch() {
        _uiState.update { 
            SearchUiState()
        }
        searchQuery.value = ""
    }
}
