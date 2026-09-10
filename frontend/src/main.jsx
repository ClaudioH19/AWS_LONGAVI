import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { configureBoneyard } from 'boneyard-js/react'
// Único punto de entrada de estilos globales de la aplicación.
import './App.css'
import './bones/registry'
import App from './App.jsx'

configureBoneyard({ select: 'viewport' })

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
